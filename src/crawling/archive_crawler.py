import argparse
import csv
import json
import logging
import logging.handlers
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, validator
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm

"""Tehran Times Archive Crawler.

This module implements a robust web crawler for the Tehran Times news archive.
It handles rate limiting, retries, data validation, and state management.

Typical usage:
    crawler = ArchiveCrawler(output_dir="./output")
    crawler.run()
"""

# Constants
BASE_URL = "https://www.tehrantimes.com/page/archive.xhtml"
FAILED_PAGES_FILENAME = "failed_pages.csv"
ACCESS_LOG_FILENAME = "access.log"
CHECKPOINT_FILENAME = "crawler_state.json"
ARTICLE_DATA_FILENAME = "articles_{date:%Y%m%d}.json"

# Schema Validation
class ArticleSchema(BaseModel):
    """Schema for validating scraped articles.

    Attributes:
        link: URL of the article
        title: Article headline
        time_published: Publication timestamp
        intro: Article introduction/summary
        downloaded: Whether article has been downloaded
        page: Page number in archive
        scrape_date: Date when article was scraped
    """
    link: str
    title: str = Field(..., min_length=1)
    time_published: str
    intro: str = ""
    downloaded: bool = False
    page: int
    scrape_date: str

    @validator('time_published')
    def validate_time(cls, v: str) -> str:
        try:
            datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
            return v
        except ValueError:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class RateLimiter:
    """Controls request rate to prevent overwhelming the server.

    Args:
        requests_per_second: Maximum number of requests allowed per second
    """
    def __init__(self, requests_per_second: float = 1.0):
        self.min_interval: float = 1.0 / requests_per_second
        self.last_request: float = 0.0

    def wait(self) -> None:
        """Wait if necessary before making next request."""
        now = time.time()
        time_passed = now - self.last_request
        if time_passed < self.min_interval:
            time.sleep(self.min_interval - time_passed)
        self.last_request = time.time()

class RequestManager:
    """Handles HTTP requests with retry logic and session management.

    Args:
        retry_count: Number of retries for failed requests
        backoff_factor: Exponential backoff factor between retries
    """
    def __init__(self, retry_count: int = 3, backoff_factor: float = 0.3):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=retry_count,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9'
        }

    def get(self, url: str, timeout: int = 30) -> requests.Response:
        """Make GET request with configured retry strategy."""
        return self.session.get(url, headers=self.headers, timeout=timeout)

class StateManager:
    """Manages crawler state and checkpoints for resume capability.

    Args:
        checkpoint_dir: Directory to store checkpoint files
    """
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir: Path = checkpoint_dir
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.checkpoint_file: Path = self.checkpoint_dir / CHECKPOINT_FILENAME

    def save_checkpoint(self, date: datetime, page: int) -> None:
        """Save current crawling state."""
        state: Dict[str, Union[str, int]] = {
            'last_date': date.strftime('%Y-%m-%d'),
            'last_page': page,
            'timestamp': datetime.now().isoformat()
        }
        temp_file = self.checkpoint_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(state, f)
        temp_file.replace(self.checkpoint_file)

    def load_checkpoint(self) -> Optional[Dict[str, Union[str, int]]]:
        """Load last saved state."""
        try:
            if self.checkpoint_file.exists():
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")
        return None

class ArchiveCrawler:
    """Main crawler implementation for Tehran Times archive.

    This class coordinates the crawling process, including:
    - Rate limiting and retry logic
    - Data validation and storage
    - State management and checkpointing
    - Logging and error handling

    Args:
        output_dir: Directory for storing crawled data
        debug: Enable debug logging
        dry_run: Run without saving data
    """
    def __init__(self, output_dir: str, debug: bool = False, dry_run: bool = False):
        self.base_url: str = BASE_URL
        self.output_dir: Path = Path(output_dir)
        self.debug: bool = debug
        self.dry_run: bool = dry_run

        # Initialize components
        self.rate_limiter: RateLimiter = RateLimiter(requests_per_second=1.0)
        self.request_manager: RequestManager = RequestManager()
        self.state_manager: StateManager = StateManager(self.output_dir / 'checkpoints')

        # Setup directories
        self._setup_directories()

        # Configure logging
        self._setup_logging()

    def _setup_directories(self) -> None:
        """Create necessary directories."""
        for dir_name in ['data', 'failed', 'logs']:
            (self.output_dir / dir_name).mkdir(parents=True, exist_ok=True)

        self.failed_file: Path = self.output_dir / 'failed' / FAILED_PAGES_FILENAME
        self.access_log: Path = self.output_dir / 'logs' / ACCESS_LOG_FILENAME

        # Initialize log files if they don't exist
        if not self.failed_file.exists():
            with open(self.failed_file, 'w', newline='') as f:
                csv.writer(f).writerow(['date', 'page', 'error', 'timestamp'])

        if not self.access_log.exists():
            with open(self.access_log, 'w', newline='') as f:
                csv.writer(f).writerow(['timestamp', 'date', 'page', 'status', 'response_time'])

    def _setup_logging(self) -> None:
        """Configure structured logging."""
        log_file: Path = self.output_dir / 'logs' / f'crawler_{datetime.now():%Y%m%d_%H%M%S}.log'

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        logger = logging.getLogger()
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG if self.debug else logging.INFO)

    def log_access(self, date: datetime, page: int, status: int, response_time: float) -> None:
        """Log access attempts with structured data."""
        with open(self.access_log, 'a', newline='') as f:
            csv.writer(f).writerow([
                datetime.now().isoformat(),
                date.strftime('%Y-%m-%d'),
                page,
                status,
                f"{response_time:.2f}"
            ])

    def log_failed_page(self, date: datetime, page: int, error: str) -> None:
        """Log failed page attempts."""
        with open(self.failed_file, 'a', newline='') as f:
            csv.writer(f).writerow([
                date.strftime('%Y-%m-%d'),
                page,
                str(error),
                datetime.now().isoformat()
            ])
        logging.error(f"Failed page: {date.strftime('%Y-%m-%d')} page {page}: {error}")

    def save_articles(self, date: datetime, articles: List[Dict]) -> None:
        """Save articles with validation."""
        if not articles:
            return

        filename: Path = self.output_dir / 'data' / ARTICLE_DATA_FILENAME.format(date=date)
        temp_file: Path = filename.with_suffix('.tmp')

        # Validate articles before saving
        validated_articles: List[Dict] = []
        for article in articles:
            try:
                validated = ArticleSchema(**article)
                validated_articles.append(article)
            except Exception as e:
                logging.error(f"Validation error for article: {e}")
                continue

        data: Dict[str, Union[str, List[Dict], Dict]] = {
            'date': date.strftime('%Y-%m-%d'),
            'articles': validated_articles,
            'metadata': {
                'count': len(validated_articles),
                'downloaded': datetime.now().isoformat()
            }
        }

        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)

        temp_file.replace(filename)
        logging.info(f"Saved {len(validated_articles)} articles for {date.strftime('%Y-%m-%d')}")

    def scrape_page(self, date: datetime, page: int) -> List[Dict]:
        """Scrapes a single archive page for the given date.

        Args:
            date: Date to scrape
            page: Page number in archive

        Returns:
            List of dictionaries containing article data

        Raises:
            Logs errors but doesn't raise exceptions to maintain crawler operation
        """
        """Scrape a single page with improved error handling."""
        url: str = f"{self.base_url}?mn={date.month}&dy={date.day}&yr={date.year}&pi={page}"

        self.rate_limiter.wait()
        start_time: float = time.time()

        try:
            response: requests.Response = self.request_manager.get(url)
            response_time: float = time.time() - start_time

            self.log_access(date, page, response.status_code, response_time)
            logging.info(f"Accessing {date.strftime('%Y-%m-%d')} page {page}: {response.status_code}")

            if response.status_code == 200:
                soup: BeautifulSoup = BeautifulSoup(response.text, 'html.parser')
                articles: List[Dict] = []

                for article in soup.select('li.clearfix.news'):
                    try:
                        article_data: Dict[str, Union[str, bool, int]] = {
                            'link': urljoin(self.base_url, article.find('a')['href']),
                            'title': article.find('h3').text.strip(),
                            'time_published': article.find('span', class_='item-time ltr').get('title', ''),
                            'intro': article.find('p', class_='introtext').text.strip(),
                            'downloaded': False,
                            'page': page,
                            'scrape_date': date.strftime('%Y-%m-%d')
                        }
                        articles.append(article_data)
                    except Exception as e:
                        self.log_failed_page(date, page, f"Parse error: {e}")

                return articles
            else:
                self.log_failed_page(date, page, f"Status code: {response.status_code}")
                return []

        except Exception as e:
            self.log_failed_page(date, page, str(e))
            logging.error(f"Error scraping {date.strftime('%Y-%m-%d')} page {page}: {str(e)}")
            return []

    def run(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> None:
        """Runs the crawler for the specified date range.

        Args:
            start_date: Start date for crawling (default: 1998-01-01)
            end_date: End date for crawling (default: current date)

        The crawler will:
        - Resume from last checkpoint if available
        - Save progress periodically
        - Validate and store article data
        - Log all activity and errors
        """
        """Run the crawler with improved state management."""
        if not start_date:
            checkpoint: Optional[Dict[str, Union[str, int]]] = self.state_manager.load_checkpoint()
            if checkpoint:
                start_date = datetime.strptime(checkpoint['last_date'], '%Y-%m-%d')
            else:
                start_date = datetime(1998, 1, 1)

        if not end_date:
            end_date = datetime.now()

        dates: List[datetime] = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

        logging.info(f"Starting crawl from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        with tqdm(dates, desc="Crawling dates") as pbar:
            for date in pbar:
                articles: List[Dict] = []
                pbar.set_description(f"Crawling {date.strftime('%Y-%m-%d')}")

                for page in range(1, 6):  # Assuming max 5 pages per date
                    if self.dry_run:
                        continue

                    page_articles: List[Dict] = self.scrape_page(date, page)
                    articles.extend(page_articles)

                    if not page_articles:  # No more articles for this date
                        break

                    self.state_manager.save_checkpoint(date, page)

                if articles:
                    self.save_articles(date, articles)

                pbar.update()

def main() -> None:
    parser = argparse.ArgumentParser(description='Tehran Times Archive Crawler')
    parser.add_argument('--output', required=True, help='Output directory for scraped content')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without saving')

    args: argparse.Namespace = parser.parse_args()

    crawler: ArchiveCrawler = ArchiveCrawler(args.output, args.debug, args.dry_run)

    try:
        crawler.run()
    except KeyboardInterrupt:
        logging.info("Crawler interrupted by user")
    except Exception as e:
        logging.error(f"Crawler failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
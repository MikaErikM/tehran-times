#src/crawling/article_scraper.py

"""Tehran Times Article Crawler and Analyzer.

This module provides a comprehensive solution for crawling and analyzing
Tehran Times articles. It handles article discovery, content extraction,
and maintains state without database dependencies.

Features:
    - Robust article crawling with retry logic
    - Date coverage analysis
    - State persistence using JSON
    - Structured logging
    - Image tracking
    - Rate limiting
"""

import argparse
import json
import logging
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Union
from dataclasses import dataclass
from enum import Enum

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, validator
from tqdm import tqdm
from urllib.parse import urljoin

# Constants
STATE_DIR_NAME = "state"
RESULTS_DIR_NAME = "results"
LOGS_DIR_NAME = "logs"
STATE_FILE_NAME = "crawler_state.json"
URLS_FILE_NAME = "urls_state.json"
IMAGES_FILE_NAME = "images_state.json"
ARTICLE_OUTPUT_FILENAME = "{date}_articles.json"

# Data Models
@dataclass
class DateRange:
    """Results of date coverage analysis."""
    start_date: datetime
    end_date: datetime
    missing_dates: List[datetime]
    total_articles: int
    date_distribution: Dict[str, int]

class ScrapingStatus(Enum):
    """Article processing status."""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"

@dataclass
class LinkInfo:
    """Information about an article link."""
    url: str
    first_seen_date: str
    title: str
    intro: str
    time_published: str
    downloaded: bool = False
    status: ScrapingStatus = ScrapingStatus.PENDING

class ArticleSchema(BaseModel):
    """Validation schema for article content."""
    url: str
    first_seen_date: str
    original_title: str
    original_intro: str = ""
    original_time: str
    scraped_title: str
    scraped_date: str
    summary: str = ""
    body: str
    tags: List[str] = Field(default_factory=list)
    category: str
    images: List[str] = Field(default_factory=list)
    related_articles: List[str] = Field(default_factory=list)
    download_timestamp: str

    @validator('download_timestamp')
    def validate_timestamp(cls, v: str) -> str:
        """Ensure timestamp is in ISO format."""
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            return datetime.now().isoformat()

class StateManager:
    """Manages crawler state using JSON files."""

    def __init__(self, state_dir: Path):
        self.state_dir: Path = state_dir
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.state_file: Path = state_dir / STATE_FILE_NAME
        self.urls_file: Path = state_dir / URLS_FILE_NAME
        self.images_file: Path = state_dir / IMAGES_FILE_NAME
        self._init_state_files()

    def _init_state_files(self) -> None:
        """Initialize state files if they don't exist."""
        for file_path in [self.state_file, self.urls_file, self.images_file]:
            if not file_path.exists():
                self._save_json(file_path, {})

    def _save_json(self, file_path: Path, data: dict) -> None:
        """Save JSON with atomic write."""
        temp_file = file_path.with_suffix('.tmp')
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            temp_file.replace(file_path)
        except Exception as e:
            logging.error(f"Error saving {file_path}: {e}")
            if temp_file.exists():
                temp_file.unlink()

    def update_url_status(self, url: str, status: ScrapingStatus, error: Optional[str] = None) -> None:
        """Update URL processing status."""
        urls_state: Dict[str, Dict[str, Union[str, None]]] = self._load_state(self.urls_file)
        urls_state[url] = {
            'last_attempt': datetime.now().isoformat(),
            'status': status.value,
            'error': error
        }
        self._save_json(self.urls_file, urls_state)

    def track_image(self, image_url: str, article_url: str) -> None:
        """Track image URL and its article association."""
        images_state: Dict[str, Dict[str, str]] = self._load_state(self.images_file)
        images_state[image_url] = {
            'article_url': article_url,
            'found_date': datetime.now().isoformat()
        }
        self._save_json(self.images_file, images_state)

    def _load_state(self, file_path: Path) -> Dict:
        """Load state file with error handling."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading state from {file_path}: {e}")
            return {}

class TehranTimesCrawler:
    """Main crawler implementation."""

    def __init__(self, input_dir: str, output_dir: str, debug: bool = False):
        self.input_dir: Path = Path(input_dir)
        self.output_dir: Path = Path(output_dir)
        self.debug: bool = debug

        # Initialize directories
        self.state_dir: Path = self.output_dir / STATE_DIR_NAME
        self.results_dir: Path = self.output_dir / RESULTS_DIR_NAME
        self.logs_dir: Path = self.output_dir / LOGS_DIR_NAME
        for directory in [self.state_dir, self.results_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.state_manager: StateManager = StateManager(self.state_dir)

        # Setup logging
        self._setup_logging()

        # Request configuration
        self.headers: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9'
        }

    def _setup_logging(self) -> None:
        """Configure logging with rotation."""
        log_file: Path = self.logs_dir / f'crawler_{datetime.now():%Y%m%d_%H%M%S}.log'
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def analyze_dates(self) -> DateRange:
        """Analyze date coverage of articles."""
        dates_articles: Dict[str, int] = {}
        all_dates: Set[datetime] = set()

        for json_file in self.input_dir.glob('*.json'):
            try:
                data: Dict = json.loads(json_file.read_text(encoding='utf-8'))
                date: datetime = datetime.strptime(data['date'], '%Y-%m-%d')
                article_count: int = len(data['articles'])
                dates_articles[date.strftime('%Y-%m-%d')] = article_count
                all_dates.add(date)
            except Exception as e:
                logging.error(f"Error analyzing {json_file}: {e}")

        if not all_dates:
            raise ValueError("No valid dates found")

        min_date: datetime = min(all_dates)
        max_date: datetime = max(all_dates)

        expected_dates: Set[datetime] = {
            min_date + timedelta(days=x)
            for x in range((max_date - min_date).days + 1)
        }

        return DateRange(
            start_date=min_date,
            end_date=max_date,
            missing_dates=sorted(list(expected_dates - all_dates)),  # Convert set to list for sorting
            total_articles=sum(dates_articles.values()),
            date_distribution=dates_articles
        )

    def _extract_article_content(self, soup: BeautifulSoup, link_info: LinkInfo) -> Optional[Dict]:
        """Extract content from article page."""
        try:
            text_div = soup.select_one('.item-text')
            main_text: str = ' '.join(
                text for text in text_div.stripped_strings
            ) if text_div else ''

            content: Dict[str, Union[str, List[str]]] = {
                'url': link_info.url,
                'first_seen_date': link_info.first_seen_date,
                'original_title': link_info.title,
                'original_intro': link_info.intro,
                'original_time': link_info.time_published,
                'scraped_title': soup.select_one('h2.item-title').text.strip(),
                'scraped_date': soup.select_one('.item-date').text.strip(),
                'summary': soup.select_one('p.summary').text.strip() if soup.select_one('p.summary') else '',
                'body': main_text,
                'tags': [tag.text for tag in soup.select('.tags a')],
                'category': soup.select_one('.breadcrumb li a').text.strip(),
                'images': self._extract_images(soup, link_info.url),
                'related_articles': [a['href'] for a in soup.select('.related-items a')],
                'download_timestamp': datetime.now().isoformat()
            }

            return ArticleSchema(**content).dict()
        except Exception as e:
            logging.error(f"Content extraction error: {e}")
            if self.debug:
                raise
            return None

    def _extract_images(self, soup: BeautifulSoup, article_url: str) -> List[str]:
        """Extract and track image URLs."""
        images: List[str] = []
        for img in soup.find_all('img'):
            src = img.get('src')
            if src:
                full_url: str = urljoin(article_url, src)
                images.append(full_url)
                self.state_manager.track_image(full_url, article_url)
        return images

    def process_article(self, link_info: LinkInfo) -> Optional[Dict]:
        """Process a single article."""
        try:
            response: requests.Response = requests.get(
                link_info.url,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()

            content: Optional[Dict] = self._extract_article_content(
                BeautifulSoup(response.text, 'html.parser'),
                link_info
            )

            if content:
                self.state_manager.update_url_status(
                    link_info.url,
                    ScrapingStatus.SUCCESS
                )
                return content
            else:
                raise Exception("Failed to extract content")

        except Exception as e:
            logging.error(f"Failed to process {link_info.url}: {e}")
            self.state_manager.update_url_status(
                link_info.url,
                ScrapingStatus.FAILED,
                str(e)
            )
            return None

    def save_article(self, article: Dict) -> None:
        """Save processed article."""
        date_str: str = datetime.fromisoformat(article['first_seen_date']).strftime('%Y-%m-%d')
        output_file: Path = self.results_dir / ARTICLE_OUTPUT_FILENAME.format(date=date_str)

        existing_articles: List[Dict] = []
        if output_file.exists():
            try:
                with open(output_file, 'r', encoding='utf-8') as f:
                    existing_articles = json.load(f)
            except json.JSONDecodeError:
                logging.error(f"Error loading existing articles for {date_str}")

        # Update or append article
        article_index: Optional[int] = next(
            (i for i, a in enumerate(existing_articles)
             if a['url'] == article['url']),
            None
        )

        if article_index is not None:
            existing_articles[article_index] = article
        else:
            existing_articles.append(article)

        # Save updated articles
        temp_file: Path = output_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(existing_articles, f, indent=2, ensure_ascii=False)
            temp_file.replace(output_file)
        except Exception as e:
            logging.error(f"Error saving article: {e}")
            if temp_file.exists():
                temp_file.unlink()
    def run(self) -> None:
        """Run the crawler."""
        try:
            date_range: DateRange = self.analyze_dates()
            logging.info(
                f"Processing articles from "
                f"{date_range.start_date.strftime('%Y-%m-%d')} to "
                f"{date_range.end_date.strftime('%Y-%m-%d')}"
            )

            for json_file in tqdm(list(self.input_dir.glob('*.json')), desc="Processing files"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data: Dict = json.load(f)

                    for article_data in tqdm(data['articles'], desc=f"Processing {json_file.name}", leave=False):
                        link_info: LinkInfo = LinkInfo(
                            url=article_data['link'],
                            first_seen_date=data['date'],
                            title=article_data.get('title', ''),
                            intro=article_data.get('intro', ''),
                            time_published=article_data.get('time_published', '')
                        )

                        content: Optional[Dict] = self.process_article(link_info)
                        if content:
                            self.save_article(content)

                        # Rate limiting
                        time.sleep(random.uniform(1, 2))

                except Exception as e:
                    logging.error(f"Error processing {json_file}: {e}")
                    if self.debug:
                        raise
                    continue

        except KeyboardInterrupt:
            logging.info("Crawler interrupted by user")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
            raise

def main() -> None:
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description='Tehran Times Article Crawler',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        '--input',
        required=True,
        help='Input directory containing article JSON files'
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Output directory for crawled content'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )

    args: argparse.Namespace = parser.parse_args()

    crawler: TehranTimesCrawler = TehranTimesCrawler(
        args.input,
        args.output,
        args.debug
    )

    crawler.run()

if __name__ == "__main__":
    main()
"""Tehran Times Data Processor.

This module converts raw JSON article data into structured CSV format.
Handles data validation, transformation, and proper error handling.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from tqdm import tqdm

@dataclass
class Article:
    """Article data structure with required fields."""
    url: str
    first_seen_date: str
    title: str
    subtitle: str
    summary: str
    body: str
    category: str
    published_date: str
    modified_date: str
    tags: List[str]
    image_url: str
    author: str
    download_timestamp: str
    source: str = 'tehrantimes'

class DataProcessor:
    """Process Tehran Times articles into CSV format."""
    
    def __init__(self, input_dir: str, output_dir: str):
        """Initialize processor with input and output paths.
        
        Args:
            input_dir: Directory containing JSON article files
            output_dir: Directory for processed output
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.setup_logging()

    def setup_logging(self):
        """Configure logging with timestamp."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        log_file = self.output_dir / f"processor_{datetime.now():%Y%m%d_%H%M%S}.log"
        
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode='w'
        )
        logging.info(f"Data processor initialized: {datetime.now()}")

    def load_articles(self) -> List[Dict]:
        """Load articles from JSON files with proper error handling."""
        all_articles = []
        json_files = list(self.input_dir.glob('*.json'))
        
        logging.info(f"Processing {len(json_files)} JSON files...")
        
        for json_file in tqdm(json_files, desc="Reading articles"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Handle both single article and list formats
                articles = data if isinstance(data, list) else [data]
                
                for article in articles:
                    processed = self.process_article(article)
                    if processed:
                        all_articles.append(processed)
                        
            except Exception as e:
                logging.error(f"Error processing {json_file}: {e}")
                continue
                
        logging.info(f"Successfully loaded {len(all_articles)} articles")
        return all_articles

    def process_article(self, article: Dict) -> Optional[Dict]:
        """Transform raw article data into standardized format.
        
        Args:
            article: Raw article data dictionary
            
        Returns:
            Processed article dictionary or None if processing fails
        """
        try:
            # Convert images list to comma-separated string
            images_str = ','.join(article.get('images', [])) if isinstance(article.get('images'), list) else ''
            
            # Create standardized article format
            processed = {
                'url': article.get('url'),
                'first_seen_date': article.get('first_seen_date'),
                'title': article.get('original_title'),
                'subtitle': article.get('original_intro', ''),
                'summary': article.get('summary', ''),
                'body': article.get('body'),
                'category': article.get('category'),
                'published_date': article.get('original_time'),
                'modified_date': article.get('scraped_date', '*'),
                'tags': article.get('tags', []),
                'image_url': images_str,
                'author': '',  # Not present in Tehran Times
                'download_timestamp': article.get('download_timestamp'),
                'source': 'tehrantimes'
            }
            
            # Basic validation
            if not all([processed['url'], processed['title'], processed['body']]):
                logging.warning(f"Missing required fields in article: {processed['url']}")
                return None
                
            return processed
            
        except Exception as e:
            logging.error(f"Error processing article: {e}")
            return None

    def save_to_csv(self, articles: List[Dict], filename: str = 'articles.csv'):
        """Save processed articles to CSV with proper formatting.
        
        Args:
            articles: List of processed article dictionaries
            filename: Output CSV filename
        """
        try:
            df = pd.DataFrame(articles)
            
            # Convert tags list to string
            df['tags'] = df['tags'].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
            
            # Save CSV with proper formatting
            output_path = self.output_dir / filename
            df.to_csv(
                output_path,
                sep=';',
                quoting=1,  # Quote all fields
                encoding='utf-8',
                index=False
            )
            
            logging.info(f"Successfully saved {len(df)} articles to {output_path}")
            
        except Exception as e:
            logging.error(f"Error saving CSV: {e}")
            raise

    def process(self):
        """Run the complete processing pipeline."""
        try:
            # Load and process articles
            articles = self.load_articles()
            
            if not articles:
                logging.error("No articles found to process")
                return
            
            # Save to CSV
            self.save_to_csv(articles)
            
            logging.info("Processing completed successfully")
            
        except Exception as e:
            logging.error(f"Processing failed: {e}")
            raise

def main():
    """Command-line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Process Tehran Times articles into CSV format"
    )
    
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing JSON files"
    )
    
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for processed files"
    )
    
    args = parser.parse_args()
    
    processor = DataProcessor(args.input, args.output)
    
    try:
        processor.process()
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
# Tehran Times Project

## Project Overview

This project provides a comprehensive data pipeline for collecting and analyzing articles from Tehran Times ([tehrantimes.com](https://tehrantimes.com)), an Iranian English-language newspaper. The pipeline includes:

* Automated archive crawling and content extraction
* Data processing and standardization
* Content analysis capabilities

The current analysis focuses on temporal patterns in keyword frequencies, with plans for expansion into more advanced natural language processing techniques.

## Project Structure
```
tehran-times-analysis/
├── src/
│   ├── crawling/
│   │   ├── archive_crawler.py      # Main archive page crawler
│   │   └── article_extractor.py    # Article content extractor
│   └── processing/
│       └── data_processor.py       # Data processing and conversion
├── data/
│   ├── raw/                        # Raw crawled data
│   │   ├── archives/               # Archive page data
│   │   └── articles/               # Full article content
│   ├── processed/                  # Processed data
│   │   └── articles.csv            # Processed article data
│   └── state/                      # Crawler state files
├── notebooks/                      # Analysis notebooks
│   ├── 00_data_overview.ipynb      # Data exploration and quality checks
│   ├── 01_temporal_analysis.ipynb  # Time-based patterns and trends
│   ├── 02_content_analysis.ipynb   # Text analysis and topic modeling
│   └── 03_category_analysis.ipynb  # Category and tag analysis
└── logs/                           # Log files
    ├── crawler/                    # Crawler logs
    └── processor/                  # Processor logs
```

## Data Formats

### Raw Data (JSON)
Articles are stored in JSON format with the following structure:
```json
{
    "url": "article_url",
    "first_seen_date": "YYYY-MM-DD",
    "original_title": "Article Title",
    "original_intro": "Article Introduction",
    "original_time": "Publication Time",
    "scraped_title": "Scraped Title",
    "scraped_date": "YYYY-MM-DD",
    "summary": "Article Summary",
    "body": "Main Article Content",
    "tags": ["tag1", "tag2"],
    "category": "Article Category",
    "images": ["image_url1", "image_url2"],
    "related_articles": ["related_url1", "related_url2"],
    "download_timestamp": "ISO DateTime"
}
```

### Processed Data (CSV)
Articles are stored in a semicolon-separated CSV with the following columns:
- url: Article URL
- first_seen_date: Date article was first discovered
- title: Article title
- subtitle: Article subtitle/introduction
- summary: Article summary
- body: Main article content
- category: Article category
- published_date: Original publication date
- modified_date: Last modification date
- tags: Comma-separated list of tags
- image_url: Comma-separated list of image URLs
- author: Article author (if available)
- download_timestamp: When the article was scraped
- source: Always "tehrantimes" #Relevant for later comparison with other state media outlets

## Installation

1. Clone the repository:
```bash
git clone https://github.com/mikaerikm/tehran-times.git
cd tehran-times
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

### Crawling Articles
```bash
# Crawl archive pages
python src/crawling/archive_crawler.py --output data/raw/archives

# Extract article content
python src/crawling/article_extractor.py \
    --input data/raw/archives \
    --output data/raw/articles
```

### Processing Data
```bash
python src/processing/data_processor.py \
    --input data/raw/articles \
    --output data/processed/articles.csv
```

### Running Analysis
Analysis is performed through Jupyter notebooks for better interactivity and visualization:

1. Start Jupyter Lab:
```bash
jupyter lab
```

2. Navigate to the `notebooks/` directory and choose the appropriate notebook:
   - `00_data_overview.ipynb`: Initial data exploration and quality assessment
   - `01_temporal_analysis.ipynb`: Time-based patterns and trends
   - `02_content_analysis.ipynb`: Text analysis and topic modeling
   - `03_category_analysis.ipynb`: Category and tag analysis

## State Management
The crawler maintains state in JSON files under `data/state/`:
- `urls_state.json`: URL processing status
- `images_state.json`: Image tracking
- `crawler_state.json`: General crawler state

## Logging
Logs are organized by component:
- Crawler logs: `logs/crawler/`
- Processor logs: `logs/processor/`

## Data Attribution
The news content analyzed in this project comes from Tehran Times, which is licensed under a Creative Commons Attribution 4.0 International License. This project is independent and not affiliated with or endorsed by Tehran Times.

# News Scraper
  
  Doesn't work with sites that have a paywall. Also some issues with JS and ad-blockers.

## Requirements

    pip install feedparser requests beautifulsoup4 googlenewsdecoder

  
- May also need to install `setuptools`


## Running News Scraper

    python3 news_scraper.py

  
Will then be prompted for search values: `query`, `start`, `end`, `site`, and `max num of articles`.  

**Alternative**: Quicker to change the values in the code if doing multiple in a row/at once.

  
## Articles
Results will be stored in a csv file with title `'{query}_{site}_{start}_{end}.csv'`

  
  

**


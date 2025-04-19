# News/Reddit

A data collection and uploading module for gathering GNews and Reddit data. This handles scraping news articles from the GNews API and processing large Reddit datasets.

## GNews Downloader
#### Desciption 

- Scrapes news articles using the GNews API and uploads them to an S3-compatible local storage directory.

- Uses terms to query GNews, and saves results in `working/gnews`.

- Deduplicates, compresses and uploads to `raw/news/gnews` in S3-local.

#### Usage

`python gnews_download.py <optional: scrape | upload>`

(Will both scrape and upload to S3 if no parameter is given.)

#### Dependencies

- Requires a `GNEWS_API_KEY` in a `.env` file.
- Assumes there is a stock ticker file in: `../Stocks/data/stock_list.csv`.
- Requires the `S3.py` util file in the `../Scripts` folder



## Reddit Raw Data Uploader
#### Description
- Processes Reddit .zst files (submissions and comments) (In this case downloaded from [here](https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10))
- Uploads them into organized S3 directories. Specifically, `raw/reddit/[submissions|comments]/`.
- Uploaded with `overwrite=False` to prevent re-uploading the same file.


#### Usage
`python upload_reddit_raw.py <path_to_downloaded_directory>`


#### Dependencies
- Expects Reddit files under `subreddits23/` inside the provided directory (This can be changed).
- Filenames should be in the format: `<subreddit>_submissions.zst` or `<subreddit>_comments.zst.`.
- Requires the `S3.py` util file in the `../Scripts` folder

----
# Stocks

A module for downloading historical stock data and generating search terms for different stocks.


## Data
### S&P 500 Constituents
Based on the S&P 500 as of 2024-12-24, and stored in `data/constituents.csv`. (Retrieved from [here](https://github.com/datasets/s-and-p-500-companies/blob/main/data/constituents.csv))

### Individual Stock Closing Prices
Downloaded from the NASDAQ API and stored in `data/daily_prices.parquet`, using the `nasdaq_download.py` script (below).

### S&P 500 Index Closing Prices
Downloaded from [investing.com](https://www.investing.com/indices/us-spx-500) (not NASDAQ due to too little history), and stored in `data/sp500_daily_prices.parquet`.

### Stock Terms
The `stock_list.csv` file is generated from `constituents.csv`, with an added `search_terms` column - a forward slash sepearated list of keywords.

This search term column was generated semi-automatically by cleaning company names and removing common corporate suffixes:


```
df['search_terms'] = df.security.str.replace("(The)", "") \
    .str.replace("Inc.", "") \
    .str.replace("(Class A)", "") \
    .str.replace("(Class B)", "") \
    .str.replace("(Class C)", "") \
    .str.replace("Company", "") \
    .str.replace("Corporation", "") \
    .str.replace("Group", "") \
    .str.replace(",", "") \
    .str.replace(" of ", " ") \
    .str.replace("plc", "") \
    .str.replace(" & ", " ") \
    .str.replace("'", "") \
    .str.replace("  ", " ") \
    .str.replace("Companies", "") \
    .str.replace(".", "")

```
Some entries were adjusted manually - eg. "Walt Disney Corporation" to "Disney".

Some companies have no search term, if their name is too generic to search or likely to come up with different companies. For these, we can still search for exact matching stock tickers.

We then used the `search_terms.py` script (overview below) to generate `search_terms.csv`, which expanded the list further by:
- Adding lowercase/uppercase variants.
- Including plural and posessive forms.
- Adding `$TICKER` and `$TICKERs` variations.

After that, some exploratory data analysis (EDA), using the `search_terms` file along with our scraped data, helped identify overly common or irrelevant terms — for example, “bro”, "target", and “cooper” appeared too frequently out of context. We manually pruned these from the list to produce `search_terms_reduced.csv`, our final, cleaned set of search terms.


## Scripts
### `nasdaq_download.py`
#### Description
- Downloads daily historical stock prices using the NASDAQ API, as described above.
- Converts and stores the data in Parquet format.
- Supports batch download using a list of symbols.
- Automatically skips symbols with unavailable data.
- Includes a short delay between API calls to prevent rate-limiting.

#### Usage
`python nasdaq_download.py <path_to_stock_list.csv> <output_path.parquet>`

### `search_terms.py`
#### Description
- Expands the seach terms list for each stock, by looking into plurals, possessives, different cases, etc.
- Combines ticker symbols, security names, and custom search terms.
- Filters out overly generic tickers (e.g. T, F, C) using an ignore list - which can be edited. 

#### Usage
`python search_terms.py <path_to_stock_list.csv> <output_path.csv>`

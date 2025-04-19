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

----

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



# Scripts

Contains general-purpose scripts for automating data processing, organization, and uploads.


---
## S3 Utils file
Custom library file used throughout the project.

Provides utilities for working with an S3 storage bucket using the boto3 SDK. Includes helpers for:
- Uploading/downloading single or multiple files
- Moving S3 objects
- Listing files by prefix

#### Dependencies
Must have a `.env` file with the required keys saved in this folder (must include S3_KEY, S3_SECRET, and S3_REGION).

---
## Symbol to Filename Mapping 
`symbol_to_filename.py` contains a python data structure that can be used to map a stock ticker to a standardised filename. Eg. `'AMD': 'Advanced_Micro_Devices_AMD'`.

Used internally when saving processed files for individual stocks or search terms.


---
## Data Processing
Contains scripts to process raw `.zst` data files from news and Reddit, converting them into clean, usable `.parquet` format files for further analysis.

Uses the `Processing/cruncher` Go module.

#### News Usage
`python process_news.py`

#### Reddit Usage
```
python3 process_reddit.py submissions      # Just posts
python3 process_reddit.py comments         # Just comments
python3 process_reddit.py all              # Both
```


---
## Data Splitting
After initial processing, data is split by mentioned company, producing smaller, company-specific artifacts.

#### Description
- Downloads all processed news files.
- Uses the Go cruncher to extract company mentions.
- Saves interim .csv files, converts to .parquet.
- Uploads final .parquet artifacts to S3.

#### News Usage
`python split_news.py`

#### Reddit Usage
`python split_reddit.py`


---
## Dataset Organisation
### `dataset_join.py`
Downloads all relevant datasets from S3 and constructs a unified parquet file containing:
- News sentiment (`processed/news/news_date_sentiment/`)
- Reddit submissions & comments sentiment (`processed/reddit/[submissions|comments]_date_sentiment/`)
- Daily prices (`marketdata/daily_prices.parquet`)
- S&P 500 index (`marketdata/sp500_daily_prices.parquet`)
#### Usage
```
python dataset_join.py download        # Downloads all datasets
python dataset_join.py join            # Joins all downloaded  into a single Parquet
python dataset_join.py                 # Does both
```

### `dataset_sort.py`
Processes sentiment parquet files by grouping entries by date and computing average sentiment per day. Outputs a new parquet file for each input, containing one row per date.

#### Usage
```
python dataset_sort.py news-all
python dataset_sort.py news <TICKER>

python dataset_sort.py reddit-submissions-all
python dataset_sort.py reddit-submissions <TICKER>

python dataset_sort.py reddit-comments-all
python dataset_sort.py reddit-comments <TICKER>
```

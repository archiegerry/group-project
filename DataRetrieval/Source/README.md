# News
  
## gnews_download.py

Downloads all the news articles from gnews


## s3.py

Uploads to S3
  

Dependencies:
- .env file with GNEWS_API_KEY
- `stock_list.csv` with `symbol` and `search_terms`
- External tools: awk, zstd

Folders:
- working/gnews/ for raw jsonl files
- S3: raw/news/gnews/

copy s3 file into directory

need to set up gnews api key.
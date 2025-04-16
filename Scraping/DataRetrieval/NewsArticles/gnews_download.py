import requests
import pandas as pd
from pathlib import Path
from dotenv import dotenv_values
from time import sleep
import json
from datetime import datetime, timedelta
import os
from s3 import *

# Parse env
file_path = Path(__file__).parent.resolve()
config = dotenv_values(file_path / ".env")

# These depend on API tier
max_requests_per_day = 24_000*4
max_articles_per_request = 100

# Requests the API, converts search terms to the native query language
def run_query(search_terms, page, from_datetime=None, to_datetime=None):
    query = " OR ".join(['"' + term + '"' for term in search_terms.split("/")])
    req = requests.get(f"https://gnews.io/api/v4/search", params=[
        ("q", query),
        ("lang", "en"),
        ("country", "us"),
        ("page", page),
        ("max", max_articles_per_request),
        ("apikey", config["GNEWS_API_KEY"]),
        ("expand", "content"),
    ] + ([("from", from_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"))] if from_datetime is not None else []) 
      + ([("to", to_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"))] if to_datetime is not None else []))
    print(req.url)
    data = req.json()

    # Delay implied by max requests
    sleep(86400 / max_requests_per_day)
    return data

# Search terms to working file path
def search_terms_path(search_terms):
    filename = "".join([x if x.isalnum() else "_" for x in search_terms])
    return Path(f"working/gnews/{filename}.jsonl") 

# Get the number of scraped articles for some search terms (some terms are shared between tickers e.g. class a/c)
def get_scraped_count(search_terms):
    path = search_terms_path(search_terms)
    if not path.exists():
        return 0
    with open(path, "rbU") as f:
        num_lines = sum(1 for _ in f)
    return num_lines

# Given the base article_counts dataframe, finds current page from file runs one query per ticker, 
def update_all_counts(article_counts):
    total_articles_col = article_counts.columns.get_loc("total_articles")
    for i, row in list(article_counts.iterrows()):
        scraped_count = get_scraped_count(row["symbol"])
        # No search terms - ignore
        if not type(row["search_terms"]) is str: continue
        # We will likely have duplicates that must be removed later
        result = run_query(row["search_terms"], page=scraped_count // max_articles_per_request)
        article_counts.iloc[i, total_articles_col] = result["totalArticles"]
    return article_counts

# Download all data for one search term
def download_term_full(search_terms, total_articles, max_articles=100_000, progress_completed=0, progress_total=None):
    # Progress tracking
    if progress_total is None: progress_total = total_articles

    scraped_count = get_scraped_count(search_terms)
    page = scraped_count // max_articles_per_request
    last_progress = -500
    
    # Note - we cant go past 1000 records for one news item, so try to approximate how many searches are needed from total_articles
    end = datetime(2024, 12, 31)
    start = datetime(2016, 1, 1) 
    step = timedelta(days=365*10)
    if total_articles > 30000:
        step = timedelta(days=7)
    elif total_articles > 10000:
        step = timedelta(days=14)
    elif total_articles > 5000:
        step = timedelta(days=28)
    elif total_articles > 1000:
        step = timedelta(days=180)
    
    # Stream to jsonl file
    with open(search_terms_path(search_terms), "a") as output:
        from_datetime = end - step
        to_datetime = end
        while to_datetime > start:

            # Run for one segment
            sub_scraped_count = 0
            sub_total_articles = 100
            page = 0
            while sub_scraped_count < sub_total_articles:
                result = run_query(search_terms, page, from_datetime=from_datetime, to_datetime=to_datetime)
                sub_total_articles = min(result.get("totalArticles", total_articles), max_articles)
                output.write("\n".join([
                    json.dumps(article) for article in result["articles"]
                ]) + "\n")
                page += 1
                sub_scraped_count += len(result["articles"])

                # Progress tracking
                scraped_count += len(result["articles"])
                progress = scraped_count + progress_completed
                if progress - last_progress > 500:
                    last_progress = progress
                    print(f"Progress {progress}/{progress_total} - downloading '{search_terms}'")

            # Move window along (backwards)
            from_datetime -= step
            to_datetime -= step

# Compress and upload all jsonl files in working directory ()
def compress_and_upload():
    for path in Path("working/gnews").glob("*.jsonl"):
        filename = path.name
        parent_path = s3_to_local_path(f"raw/news/gnews/")
        parent_path.mkdir(exist_ok=True, parents=True)
        file_path = parent_path / f"{filename}.zst"
        if not file_path.exists():
            # Remove all duplicate lines, pipe to zst in s3 path
            os.system(f"awk '!seen[$0]++' {path} | zstd -o {file_path}")

    upload_all("raw/news/gnews")


def main():
    # Create working directory
    working_dir = Path("working/gnews")
    working_dir.mkdir(exist_ok=True, parents=True)

    # Load base 
    stocks = pd.read_csv("Scraping/stock_list.csv") # TODO: change path

    # Load number of articles for each stock
    article_counts_path = Path("working/gnews/article_counts.csv")
    article_counts = pd.DataFrame()
    if article_counts_path.exists():
        article_counts = pd.read_csv(article_counts_path)
    else:
        article_counts = stocks.copy()
        article_counts["total_articles"] = 0

        # Update and save before scraping
        update_all_counts(article_counts)
        article_counts.to_csv(article_counts_path, index=False)

    # Start scraping each search term, will be joined with tickers when zipping
    total_articles = article_counts.total_articles.sum()
    progress = 0
    for _, row in article_counts.iterrows():
        if type(row["search_terms"]) is not str or len(row["search_terms"]) == 0:
            print(f"No search terms {row}")
            continue
        # Have to do it in one sitting with windowed scraping
        path = search_terms_path(row["search_terms"])
        if not path.exists():
            download_term_full(row["search_terms"], row["total_articles"], progress_completed=progress, progress_total=total_articles)
        else:
            print(f"Already downloaded {row['search_terms']}")

        progress += row["total_articles"]
    

if __name__ == "__main__":
    # main()
    compress_and_upload()
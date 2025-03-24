from s3 import s3_to_local_path, download_all, upload, download, upload_all
import sys, os
from pathlib import Path
import pandas as pd
from pathlib import Path
from datetime import date

"""

Download all datasets from S3, construct a parquet file with all data joined.
Includes:
    - processed/news/news_date_sentiment/*.parquet
    - processed/reddit/submissions_date_sentiment/*.parquet
    - processed/reddit/comments_date_sentiment/*.parquet
    - marketdata/daily_prices.parquet
    - marketdata/sp500_daily_prices.parquet

USAGE:
python3 Scripts/join_datasets.py

"""

def download_datasets():
    s3_to_local_path("processed/news/news_date_sentiment/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/submissions_date_sentiment/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/comments_date_sentiment/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("marketdata/").mkdir(parents=True, exist_ok=True)

    # Upload local files without overwrite
    download_all("processed/news/news_date_sentiment/", overwrite=False)
    download_all("processed/reddit/submissions_date_sentiment/", overwrite=False)
    download_all("processed/reddit/comments_date_sentiment/", overwrite=False)
    download_all("marketdata/", overwrite=False)


def join_datasets():
    news_dfs = []
    for path in s3_to_local_path("processed/news/news_date_sentiment/").glob("*.parquet"):
        news_dfs.append(pd.read_parquet(path).reset_index().assign(symbol=path.stem))
    news = pd.concat(news_dfs, ignore_index=True).reset_index(drop=True)
    news["dt"] = pd.to_datetime(news.dt).dt.date.astype(str)

    submissions_dfs = []
    for path in s3_to_local_path("processed/reddit/submissions_date_sentiment/").glob("*.parquet"):
        submissions_dfs.append(pd.read_parquet(path).reset_index().assign(symbol=path.stem))
    submissions = pd.concat(submissions_dfs, ignore_index=True).reset_index(drop=True)
    submissions["dt"] = pd.to_datetime(submissions.dt).dt.date.astype(str)

    comments_dfs = []
    for path in s3_to_local_path("processed/reddit/comments_date_sentiment/").glob("*.parquet"):
        comments_dfs.append(pd.read_parquet(path).reset_index().assign(symbol=path.stem))
    comments = pd.concat(comments_dfs, ignore_index=True).reset_index(drop=True)
    comments["dt"] = pd.to_datetime(comments.dt).dt.date.astype(str)


    prices = pd.read_parquet(s3_to_local_path("marketdata/daily_prices.parquet"))
    sp500_prices = pd.read_parquet(s3_to_local_path("marketdata/sp500_daily_prices.parquet"))

    # Duplicate date range for each symbol
    date_range_dfs = []
    symbols = news.symbol.unique()
    for symbol in symbols:
        date_range_dfs.append(
            pd.DataFrame({
                "dt": pd.date_range(date(2004, 1, 1), date(2025, 1, 1)),
                "symbol": symbol,
            }),
        )
    df = pd.concat(date_range_dfs, ignore_index=True).reset_index(drop=True)
    df["dt"] = df.dt.dt.date.astype(str)


    sentiment_column = "roberta_normalised_compound"


    df = df.merge(news[["symbol", "dt", sentiment_column]].rename(columns={
        sentiment_column: "news_sentiment",
    }), how="left", on=["symbol", "dt"])
    df = df.merge(submissions[["symbol", "dt", sentiment_column]].rename(columns={
        sentiment_column: "submissions_sentiment",
    }), how="left", on=["symbol", "dt"])
    df = df.merge(comments[["symbol", "dt", sentiment_column]].rename(columns={
        sentiment_column: "comments_sentiment",
    }), how="left", on=["symbol", "dt"])

    # Prices
    prices["date"] = prices.date.dt.date.astype(str)
    df = df.merge(prices[["symbol", "date", "open", "close", "high", "low"]].rename(columns={
        "date": "dt", 
    }), how="left", on=["symbol", "dt"])

    # Index prices
    sp500_prices["Date"] = pd.to_datetime(sp500_prices.Date, dayfirst=True)
    sp500_prices["Date"] = sp500_prices.Date.dt.date.astype(str)
    sp500_prices["Open"] = sp500_prices.Open.str.replace(",", "").astype(float)
    sp500_prices["Price"] = sp500_prices.Price.str.replace(",", "").astype(float)
    df = df.merge(sp500_prices[["Date", "Open", "Price"]].rename(columns={
        "Date": "dt", "Open": "sp500_open", "Price": "sp500_close",
    }), how="left", on=["dt"])

    s3_to_local_path("datasets/").mkdir(exist_ok=True, parents=True)
    path = s3_to_local_path("datasets/roberta.parquet")
    df.to_parquet(path)
    upload(path)


if __name__ == "__main__":
    # download_datasets()
    join_datasets()
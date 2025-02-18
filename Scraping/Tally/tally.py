import pandas as pd
import re
import sys
import ast
from s3 import *

# read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    tickers_df['search_terms'] = tickers_df['search_terms'].str.split('|')
    return tickers_df

# read in reddit submission parquet file as dataframe
def read_reddit_parquet(path):
    df = pd.read_parquet(path) 
    df['text'] = df['title'].fillna('') + " " + df['body'].fillna('')  
    return df

# read in news article parquet file as dataframe
def read_news_parquet(path):
    df = pd.read_parquet(path)
    df['text'] = df['title'].fillna('') + " " + df['description'].fillna('') + " " + df['body'].fillna('') 
    return df

# tally mentions of each stock ticker in reddit posts
def count_tickers(df, tickers):
    counts = {}

    # iterate through each ticker
    for _, row in tickers.iterrows():
        symbol, search_terms = row['symbol'], row['search_terms']

        # Sum up occurrences of each term
        ticker_counts = df['text'].str.count(search_terms[0])
        for term in search_terms[1:]:
            ticker_counts += df['text'].str.count(term)

        counts[symbol] = ticker_counts
    
    return pd.DataFrame(counts)

def news_all(tickers): 
    #s3_to_local_path("processed/news/gnews").mkdir(parents=True, exist_ok=True)
    #s3_to_local_path("processed/news/tally").mkdir(parents=True, exist_ok=True)

    download_all("processed/news", overwrite=False)

    submission_paths = s3_to_local_path("processed/news/gnews/").glob("*.parquet")

    # Loop all news and process mentions per-parquet
    for path in tqdm(submission_paths):
        download_all("processed/news/tally", overwrite=False)
        output_path = s3_to_local_path(f"processed/news/tally/{path.stem}_tally.parquet")
        if output_path.is_file():
            continue
        else:
            with open(output_path, 'w') as file:
                upload(f"processed/news/tally/{path.stem}_tally.parquet")

            print(f'Proce sing: {output_path}')
            try:
                df = read_news_parquet(path)
            except:
                print(f"Couldn't process {path}")
                continue
            counts = count_tickers(df, tickers)
            counts.to_parquet(output_path)

            print('Completed processing, uploading...')
            upload(f"processed/news/tally/{path.stem}_tally.parquet")
            print('Upload complete.')

def main():

    stock_file = sys.argv[1]
    
    # read in stock tickers
    tickers = read_tickers(stock_file)
   
    # read in news articles or reddit submissions depending on cmd line arg
    if sys.argv[2] == "news":
        input_file = sys.argv[3]
        output_file = sys.argv[4]
        df = read_news_parquet(input_file)
        # count mentions of each stock ticker in news articles
        counts = count_tickers(df, tickers)
        # write counts to parquet file
        counts.to_parquet(output_file)

    elif sys.argv[2] == "reddit":
        input_file = sys.argv[3]
        output_file = sys.argv[4]
        df = read_reddit_parquet(input_file)
        # count mentions of each stock ticker in news articles
        counts = count_tickers(df, tickers)
        # write counts to parquet file
        counts.to_parquet(output_file)

    elif sys.argv[2] == "reddit-all":
        print("Unfinished")
    elif sys.argv[2] == "news-all":
        news_all(tickers)
        #print(list('processed/news'))
    else:
        print("Usage: python tally.py <stock_file> <news/reddit/news-all/reddit-all> <input_file_optional> <output_file_optional>")
        sys.exit(1)


if __name__ == "__main__":
    main()
    
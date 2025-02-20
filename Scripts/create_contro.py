from s3 import *
import sys, os
from tqdm import tqdm
import pandas as pd
import numpy as np

# Tally 'Row' has a 1-1 correlation with texttable row (an article or post/comment)
# Function returns a list of the rows per parquet where more than one company is mentioned

def find_contro(tally):
    # Count nonzero values per row
    row_counts = np.count_nonzero(tally.values, axis=1)

    # Find row indices where there are at least 2 nonzero values
    row_indices = np.where(row_counts >= 2)[0]  # Get indices as an array

    return row_indices

def main():
    # Get all the files from S3 (don't have the reddit tallies yet) 
    download_all("processed/news/tally_filtered")
    download_all("processed/news/gnews_filtered")

    # Get all filenames 
    gnews_paths = s3_to_local_path("processed/news/gnews_filtered/").glob("*.parquet")

    # Include when reddit tallies get added
    #download_all("processed/reddit/tally/")
    #download_all("processed/news/gnews_filtered/")
    #reddit_submission_paths = s3_to_local_path("processed/news/gnews_filtered/").glob("*.parquet")

    # ALSO WRITE CODE TO DEAL WITH REDDIT

    for path in tqdm(gnews_paths):
        news = pd.read_parquet(path)
        tally = pd.read_parquet(s3_to_local_path(f"processed/news/tally_filtered/{path.stem}_tally.parquet"))
        print(f'News file: {path}\n')
        contro_rows = find_contro(tally=tally)

        # Create new df with only controversial rows
        news_contro = news.loc[contro_rows]
        tally_contro = tally.loc[contro_rows]

        # Remove controversial rows from original 
        news_cleaned = news.drop(index=contro_rows)
        tally_cleaned = tally.drop(index=contro_rows)

        # Reset index
        news_cleaned = news_cleaned.reset_index(drop=True) 
        tally_cleaned = tally_cleaned.reset_index(drop=True)
        news_contro = news_contro.reset_index(drop=True)
        tally_contro = tally_contro.reset_index(drop=True)

        s3_to_local_path("processed/news/contro").mkdir(parents=True, exist_ok=True)

        # Overwrite old parquets with clean files
        os.remove(s3_to_local_path(f'processed/news/gnews_filtered/{path.stem}.parquet'))
        os.remove(s3_to_local_path(f'processed/news/tally_filtered/{path.stem}_tally.parquet'))
        news_cleaned.to_parquet(s3_to_local_path(f'processed/news/gnews_filtered/{path.stem}.parquet'))
        tally_cleaned.to_parquet(s3_to_local_path(f'processed/news/tally_filtered/{path.stem}_tally.parquet'))

        # Save controversial articles to new path
        news_contro.to_parquet(s3_to_local_path(f'processed/news/contro/{path.stem}_contro.parquet'))
        tally_contro.to_parquet(s3_to_local_path(f'processed/news/contro/{path.stem}_tally_contro.parquet'))
        
    
if __name__ == "__main__":
    #main()
    paths = s3_to_local_path('processed/news/gnews_filtered').glob("*.parquet")
    sortedpaths = sorted(paths)
    for path in sortedpaths:
        print(path)

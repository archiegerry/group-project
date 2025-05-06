import sys
from pathlib import Path
scripts_folder = os.path.join(os.getcwd(), 'Scripts')
sys.path.append(scripts_folder)
from s3 import *
import pandas as pd


"""
Groups sentiment data by date and saves to new parquet file.

USAGE:
    python dataset_join.py news-all
        Process all news sentiment files and group by date.
        
    python dataset_join.py news <TICKER>
        Process a single news file for the given <TICKER>.

    python dataset_join.py reddit-submissions-all
        Process all Reddit submission sentiment files and group by date.

    python dataset_join.py reddit-submissions <TICKER>
        Process a single Reddit submission file for the given <TICKER>.

    python dataset_join.py reddit-comments-all
        Process all Reddit comment sentiment files and group by date.

    python dataset_join.py reddit-comments <TICKER>
        Process a single Reddit comment file for the given <TICKER>.
"""

def all_files(file_path):
    if "news" in file_path:
        output_dir = "processed/news/news_date_sentiment"
    elif "submissions" in file_path:
        output_dir = "processed/reddit/submissions_date_sentiment"
    elif "comments" in file_path:
        output_dir = "processed/reddit/comments_date_sentiment"
    else:
        raise ValueError("Unrecognized file path.")
    # Get all files in directory
    dfs = [
        (f, pd.read_parquet(f)) for f in s3_to_local_path(file_path).glob("*")
    ]
    for path, df in tqdm(dfs):
        out_path = s3_to_local_path(f"{output_dir}/{path.stem}.parquet")
        out_path.parent.mkdir(exist_ok=True, parents=True)
        if not out_path.exists():
            print(out_path)
            if "reddit" in file_path:
                # Rename datetime to dt
                df = df.rename(columns={"datetime": "dt"})
            df = df[["dt", "roberta_pos", "roberta_neu", "roberta_neg", "roberta_compound", "roberta_normalised_compound"]]        
            df["dt"] = pd.to_datetime(df["dt"], unit="ms").dt.date        
            # Get average sentiment for each day
            df = df.groupby("dt").mean()
            df.to_parquet(out_path)


# One file at a time
def single_file(file_path, ticker):
    if "news" in file_path:
        output_dir = "processed/news/news_date_sentiment"
    elif "submissions" in file_path:
        output_dir = "processed/reddit/submissions_date_sentiment"
    elif "comments" in file_path:
        output_dir = "processed/reddit/comments_date_sentiment"
    else:
        raise ValueError("Unrecognized file path.")
    path = s3_to_local_path(f"{file_path}/{ticker}.parquet")
    df = pd.read_parquet(path)
    df = df[["dt", "roberta_pos", "roberta_neu", "roberta_neg", "roberta_compound", "roberta_normalised_compound"]]
    # Convert to datetime
    df["dt"] = pd.to_datetime(df["dt"], unit="ms").dt.date
    # Get average sentiment for each day
    df = df.groupby("dt").mean()
    df.to_parquet(s3_to_local_path(f"{output_dir}/{ticker}.parquet"))
    return


def get_from_s3():    
    download_all("processed/news/twitter_roberta/", overwrite=False)
    download_all("processed/news/news_date_sentiment/", overwrite=False)
    download_all("processed/reddit/comments_twitter_roberta/", overwrite=False)
    download_all("processed/reddit/comments_date_sentiment/", overwrite=False)
    download_all("processed/reddit/submissions_twitter_roberta/", overwrite=False)
    download_all("processed/reddit/submissions_date_sentiment/", overwrite=False)


def put_to_s3():
    upload_all("processed/news/news_date_sentiment/", overwrite=False)
    upload_all("processed/reddit/submissions_date_sentiment/", overwrite=False)
    upload_all("processed/reddit/comments_date_sentiment/", overwrite=False)


def main():
    get_from_s3()
    if sys.argv[1] == "news-all":
        all_files("processed/news/twitter_roberta/")
    elif sys.argv[1] == "news":
        single_file("processed/news/twitter_roberta/", sys.argv[2])
    elif sys.argv[1] == "reddit-submissions-all":
        all_files("processed/reddit/submissions_twitter_roberta/")
    elif sys.argv[1] == "reddit-submissions":
        single_file("processed/reddit/submissions_twitter_roberta/", sys.argv[2])
    elif sys.argv[1] == "reddit-comments-all":
        all_files("processed/reddit/comments_twitter_roberta/")
    elif sys.argv[1] == "reddit-comments":
        single_file("processed/reddit/comments_twitter_roberta/", sys.argv[2])
    else:
        print("Invalid command")
    put_to_s3()

if __name__ == "__main__":
    main()
import sys
from pathlib import Path
from s3 import *
import pandas as pd



def all_files(file_path):
    if "news" in file_path:
        output_dir = "processed/news/news_date_sentiment"
    elif "submissions" in file_path:
        output_dir = "processed/reddit/submissions_date_sentiment"
    elif "comments" in file_path:
        output_dir = "processed/reddit/comments_date_sentiment"
    else:
        raise ValueError("Unrecognized file path.")
    
    # get all files in directory
    dfs = [
        (f, pd.read_parquet(f)) for f in s3_to_local_path(file_path).glob("*")
    ]
    for path, df in tqdm(dfs):
        out_path = s3_to_local_path(f"{output_dir}/{path.stem}.parquet")
        out_path.parent.mkdir(exist_ok=True, parents=True)
        if not out_path.exists():
            print(out_path)
            
            if "reddit" in file_path:
                # rename datetime to dt
                df = df.rename(columns={"datetime": "dt"})
                
            df = df[["dt", "roberta_pos", "roberta_neu", "roberta_neg", "roberta_compound", "roberta_normalised_compound"]]        
            df["dt"] = pd.to_datetime(df["dt"], unit="ms").dt.date        
            # get average sentiment for each day
            df = df.groupby("dt").mean()
            # save to new parquet file
            df.to_parquet(out_path)



def single_file(file_path, ticker):
    if "news" in file_path:
        output_dir = "processed/news/news_date_sentiment"
    elif "submissions" in file_path:
        output_dir = "processed/reddit/submissions_date_sentiment"
    elif "comments" in file_path:
        output_dir = "processed/reddit/comments_date_sentiment"
    else:
        raise ValueError("Unrecognized file path.")
    
    # get path needed
    path = s3_to_local_path(f"{file_path}/{ticker}.parquet")
    # read in parquet file
    df = pd.read_parquet(path)
    # only keep the columns we need
    df = df[["dt", "roberta_pos", "roberta_neu", "roberta_neg", "roberta_compound", "roberta_normalised_compound"]]
    # convert to datetime
    df["dt"] = pd.to_datetime(df["dt"], unit="ms").dt.date
    
    # get average sentiment for each day
    df = df.groupby("dt").mean()
    # save to new parquet file
    df.to_parquet(s3_to_local_path(f"{output_dir}/{ticker}.parquet"))
    return






def get_from_s3():
    # uncomment whatever is needed:
    
    # -----NEWS-----
    # download_all("processed/news/twitter_roberta/", overwrite=False)
    download_all("processed/news/news_date_sentiment/", overwrite=False)
    # -----REDDIT COMMENTS-----
    # download_all("processed/reddit/comments_twitter_roberta/", overwrite=False)
    download_all("processed/reddit/comments_date_sentiment/", overwrite=False)
    # -----REDDIT SUBMISSIONS-----
    # download_all("processed/reddit/submissions_twitter_roberta/", overwrite=False)
    download_all("processed/reddit/submissions_date_sentiment/", overwrite=False)
    
    # make directories if they don't exist (should exist already!)
    # s3_to_local_path("processed/news/news_date_sentiment/").mkdir(parents=True, exist_ok=True)
    # s3_to_local_path("processed/reddit/submissions_date_sentiment/").mkdir(parents=True, exist_ok=True)
    # s3_to_local_path("processed/reddit/comments_date_sentiment/").mkdir(parents=True, exist_ok=True)
    
    
def put_to_s3():
    # uncomment whatever is needed:
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
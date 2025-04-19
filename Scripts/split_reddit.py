scripts_folder = os.path.join(os.getcwd(), 'Scripts')
sys.path.append(scripts_folder)
from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path
import re

"""

Split reddit files into artifacts based on which company is mentioned.

USAGE:
    python split_reddit.py

"""

# Uploaded zsts have a .jsonl at the end of the file name
def remove_jsonl_suffix(path):
    jsonl_removed = str(path).replace(".jsonl", "")
    return jsonl_removed

def split_reddit():
    # Download all processed parquet files 
    s3_submissions_paths = download_all("processed/reddit/submissions/")
    s3_comments_paths = download_all("processed/reddit/comments/")
    submissions_path_artifacts = s3_to_local_path("processed/reddit/submissions_artifacts/")
    comments_path_artifacts = s3_to_local_path("processed/reddit/comments_artifacts/")
    submissions_path_artifacts.mkdir(exist_ok=True, parents=True)
    comments_path_artifacts.mkdir(exist_ok=True, parents=True)

    # Remove all existing files (technically not an issue since we dedupe but save spaces)
    for path in submissions_path_artifacts.glob("*.csv"):
        os.remove(path)
    for path in comments_path_artifacts.glob("*.csv"):
        os.remove(path) 
            
    # Hit them with the ol' Crunchertron 3000
    for path in tqdm(s3_submissions_paths):
        local_path = s3_to_local_path(path)
        cmd = f"./Processing/cruncher/cruncher split-reddit {local_path} {submissions_path_artifacts}{os.sep}"
        print(cmd)
        os.system(cmd)
        print(f"{path.split('/')[-1]} done.")    

    # Convert all the csvs
    for path in tqdm(submissions_path_artifacts.glob("*.csv")):
        df = pd.read_csv(path, names="post_id,text,domain,flair,subreddit,score,downs,datetime".split(","))
        df = df.drop_duplicates()
        df.to_parquet(str(path).replace(".csv", ".parquet"))

    for path in tqdm(comments_path_artifacts.glob("*.csv")):
        df = pd.read_csv(path, names="comment_id,text,score,datetime,parent_id,start_ticker,post_id,flair,subreddit,post_score,post_downs,post_datetime".split(","))
        df = df.drop_duplicates()
        df.to_parquet(str(path).replace(".csv", ".parquet"))

    # Remove all csv files 
    for path in submissions_path_artifacts.glob("*.csv"):
        os.remove(path)
    for path in comments_path_artifacts.glob("*.csv"):
        os.remove(path) 

    # Upload just the parquets
    upload_all("processed/reddit/submissions_artifacts/")
    upload_all("processed/reddit/comments_artifacts/")

if __name__ == "__main__":
    split_reddit()

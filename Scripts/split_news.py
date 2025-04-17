from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path
import re

"""

Split news files into artifacts based on which company is mentioned.

USAGE:
    python split_news.py

"""

# Uploaded zsts have a .jsonl at the end of the file name
def remove_jsonl_suffix(path):
    jsonl_removed = str(path).replace(".jsonl", "")
    return jsonl_removed

def split_news():
    # Download all processed parquet files 
    s3_paths = download_all("processed/news/gnews/")
    output_path_artifacts = s3_to_local_path("processed/news/gnews_artifacts/")

    # Remove all existing files (technically not an issue since we dedupe but save spaces)
    for path in output_path_artifacts.glob("*.csv"):
        os.remove(path)

    # Hit them with the ol' Crunchertron 3000
    for path in tqdm(s3_paths):
        local_path = s3_to_local_path(path)
        cmd = f"./Processing/cruncher/cruncher split-news {local_path} {output_path_artifacts}{os.sep}"
        print(cmd)
        os.system(cmd)
        print(f"{path.split('/')[-1]} done.")    

    # Convert all the csvs
    for path in tqdm(output_path_artifacts.glob("*.csv")):
        df = pd.read_csv(path, names=["url", "text", "domain", "dt"])
        df = df.drop_duplicates()
        df.to_parquet(str(path).replace(".csv", ".parquet"))

    # Remove all csv files 
    for path in output_path_artifacts.glob("*.csv"):
        os.remove(path)

    # Upload just the parquets
    upload_all("processed/news/gnews_artifacts/")

if __name__ == "__main__":
    split_news()

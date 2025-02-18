from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path
import re

"""

Process news zst files into parquet format.
Downloads zst files, processes locally and uploads parquets.

"""
# Uploaded zsts have a .jsonl at the end of the file name
def remove_jsonl_suffix(path):
    jsonl_removed = str(path).replace(".jsonl", "")
    return jsonl_removed

def process_news():
    # Download all raw zst files
    s3_paths = download_all("raw/news/gnews/")
    
    # Hit them with the ol' Crunchertron 3000
    for path in tqdm(s3_paths):
        local_path = s3_to_local_path(path)
        renamed_path = remove_jsonl_suffix(local_path)
        #os.rename(local_path, renamed_path)
        stem = re.search(r'^.*\/([^\/]+)\.zst$', renamed_path)
        output_path = s3_to_local_path(f"processed/news/gnews/{stem.group(1)}.parquet")
        if not output_path.exists():
            print(f"{stem.group(1)}...")
            os.system(f"zstdcat {local_path} | ./Scraping/Cruncher/cruncher/cruncher news-articles {output_path}")
            upload(f"processed/news/gnews/{stem.group(1)}.parquet")
            print(f"{stem.group(1)} Uploaded.")    

if __name__ == "__main__":
    process_news()

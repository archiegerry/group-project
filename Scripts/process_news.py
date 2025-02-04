from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path

"""

Process news zst files into parquet format.
Downloads zst files, processes locally and uploads parquets.

"""

def process_news():
    # Download all raw zst files
    s3_paths = download_all('raw/news/gnews/')
    # Process each file locally, for example with cruncher
    # (note that in this case must run from same dir as cruncher executable)
    for s3_path in s3_paths:
       local_path = s3_to_local_path(s3_path)
       # Probably not good practice but quick way to get output dir
       output_path = s3_to_local_path(s3_path.replace("raw/", "processed/"))
       # Ensure output path exists
       output_path.parents[0].mkdir(parents=True, exist_ok=True)
       # Run job
       os.system(f"zstdcat {local_path} | ./cruncher reddit-submissions {output_path}")
    # Upload local processed files
    upload_all("processed/reddit/submissions/", overwrite=False)

if __name__ == "__main__":
    process_news()
    

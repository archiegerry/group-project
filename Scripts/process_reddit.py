from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path

"""

Process reddit torrent zst files into parquet.
By default just processes locally downloaded files, uploads parquets.
Must be invoked from group-project dir.

If you want to update the 

"""

def process_reddit():
    s3_to_local_path("processed/reddit/submissions").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/comments").mkdir(parents=True, exist_ok=True)

    # Process submissions locally
    print("Processing and uploading submissions...")
    submission_paths = s3_to_local_path("raw/reddit/submissions").glob("*.zst")
    for path in tqdm(submission_paths):
        output_path = s3_to_local_path(f"processed/reddit/submissions/{path.stem}.parquet")
        if not output_path.exists():
            print(f"{path.stem}...")
            os.system(f"zstdcat {path} | ./Scraping/Cruncher/cruncher/cruncher reddit-submissions {output_path}")
            upload(f"processed/reddit/submissions/{path.stem}.parquet")
            print(f"{path.stem} Uploaded.")

    # Process comments locally
    print("Processing and uploading comments...")
    comment_paths = s3_to_local_path("raw/reddit/comments").glob("*.zst")
    for path in tqdm(comment_paths):
        output_path = s3_to_local_path(f"processed/reddit/comments/{path.stem}.parquet")
        if not output_path.exists():
            print(f"{path.stem}...")
            os.system(f"zstdcat {path} | ./Scraping/Cruncher/cruncher/cruncher reddit-comments {output_path}")
            upload(f"processed/reddit/comments/{path.stem}.parquet")
            print(f"{path.stem} Uploaded.")


    # Upload any not uploaded files 
    print("Uploading missing files...")
    upload_all("processed/reddit/submissions", overwrite=False)
    upload_all("processed/reddit/comments", overwrite=False)

if __name__ == "__main__":
    process_reddit()
    

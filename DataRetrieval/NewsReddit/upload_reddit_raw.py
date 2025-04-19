import sys
import sys, os
from pathlib import Path
scripts_folder = os.path.join(os.getcwd(), '..','..', 'Scripts')
sys.path.append(scripts_folder)
from s3 import *

"""
Upload reddit torrent zst files.
Finds comments and submissions files in the download directory (e.g. ~/Downloads/reddit), moves them to s3local and uploads them.

USAGE:
python upload_reddit_raw.py <path_to_download_directory>

"""

def upload_reddit(download_path):
    # Check if the download path exists and is directory
    if not download_path.exists() or not download_path.is_dir():
        print(f"Error: download path {download_path} is not a valid directory")
        return
    
    # Ensure S3-local directories exist
    s3_to_local_path("raw/reddit/submissions").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("raw/reddit/comments").mkdir(parents=True, exist_ok=True)

    # List all files
    paths = download_path.glob("subreddits23/*.zst")
    for path in paths:
        parts = path.stem.split("_")
        if len(parts) != 2 or parts[1] not in ["submissions", "comments"]:
            print(f"Invalid file: {path} - expected subreddit_comments.zst or subreddit_submissions.zst")
            continue

        # Move to s3 local 
        subreddit, data_type = parts
        local_path = s3_to_local_path(Path("raw/reddit") / data_type / (subreddit + ".zst"))
        os.replace(path, local_path)
        print(f"mv {path} {local_path}")
        
    # Upload local files without overwrite
    upload_all("raw/reddit/submissions/", overwrite=False)
    upload_all("raw/reddit/comments/", overwrite=False)


if __name__ == "__main__":
    if len(sys.argv) < 2: 
        print("Usage: python upload_reddit_raw.py <path_to_download_directory>")
        sys.exit(1)
    
    download_path = Path(sys.argv[1])
    upload_reddit(download_path)
    

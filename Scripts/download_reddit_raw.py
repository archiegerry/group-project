from s3 import *
import sys, os
from pathlib import Path

"""

Download all processed reddit comments and submissions files, or just the posts.
USAGE:
python3 Scripts/download_reddit_raw.py submissions        FOR JUST POSTS
python3 Scripts/download_reddit_raw.py all                FOR EVERYTHING

"""

def download_reddit_all():
    s3_to_local_path("raw/reddit/").mkdir(parents=True, exist_ok=True)

    # Upload local files without overwrite
    download_all("raw/reddit/", overwrite=False)

def download_reddit_submissions():
    s3_to_local_path("raw/reddit/submissions").mkdir(parents=True, exist_ok=True)

    # Upload local files without overwrite
    download_all("raw/reddit/submissions", overwrite=False)

if __name__ == "__main__":
    download_choice = Path(sys.argv[1])
    print(download_choice)
    if (str(download_choice) == "all"):
        print("Downloading everything")
        download_reddit_all()
        print(f"Done. Files can be found in: ")
        print(f"{s3_to_local_path('raw/reddit/submissions')}")
        print(f"{s3_to_local_path('raw/reddit/comments')}")
    elif (str(download_choice) == 'submissions'):
        print("Downloading just submissions")
        download_reddit_submissions()
        print(f"Done. Files can be found in: ")
        print(f"{s3_to_local_path('raw/reddit/submissions')}")

    

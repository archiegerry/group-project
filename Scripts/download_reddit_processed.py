from s3 import *
import sys, os
from pathlib import Path

"""

Download *all* processed reddit comments and submissions files.

"""

def download_reddit():
    s3_to_local_path("processed/reddit/submissions").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/comments").mkdir(parents=True, exist_ok=True)

    # Upload local files without overwrite
    download_all("processed/reddit/submissions/", overwrite=False)
    download_all("processed/reddit/comments/", overwrite=False)

if __name__ == "__main__":
    download_reddit()
    print(f"Done. Files can be found in: ")
    print(f"{s3_to_local_path('processed/reddit/submissions')}")
    print(f"{s3_to_local_path('processed/reddit/comments')}")
    

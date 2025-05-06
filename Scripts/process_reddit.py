scripts_folder = os.path.join(os.getcwd(), 'Scripts')
sys.path.append(scripts_folder)
from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path

"""

Process reddit torrent zst files into parquet and upload to S3.
By default just processes locally downloaded files, uploads parquets.
Must be invoked from group-project dir.

USAGE:
python3 Scripts/process_reddit.py submissions        FOR JUST POSTS
python3 Scripts/process_reddit.py comments           FOR JUST COMMENTS
python3 Scripts/process_reddit.py all                FOR EVERYTHING


"""

def process_reddit_all():
    process_reddit_submissions()
    process_reddit_comments()

def process_reddit_submissions():
    s3_to_local_path("processed/reddit/submissions").mkdir(parents=True, exist_ok=True)

    # Process submissions locally
    print("Processing and uploading submissions...")
    submission_paths = s3_to_local_path("raw/reddit/submissions").glob("*.zst")
    for path in tqdm(submission_paths):
        output_path = s3_to_local_path(f"processed/reddit/submissions/{path.stem}.parquet")
        if not output_path.exists():
            print(f"{path.stem}...")
            os.system(f"zstdcat {path} | ./Processing/cruncher/cruncher reddit-submissions {output_path}")
            upload(f"processed/reddit/submissions/{path.stem}.parquet")
            print(f"{path.stem} Uploaded.")


    # Upload any not uploaded files 
    print("Uploading missing files...")
    upload_all("processed/reddit/submissions", overwrite=False)

def process_reddit_comments():
    s3_to_local_path("processed/reddit/comments").mkdir(parents=True, exist_ok=True)

    # Process submissions locally
    print("Processing and uploading comments...")
    submission_paths = s3_to_local_path("raw/reddit/comments").glob("*.zst")
    for path in tqdm(submission_paths):
        output_path = s3_to_local_path(f"processed/reddit/comments/{path.stem}.parquet")
        if not output_path.exists():
            print(f"{path.stem}...")
            os.system(f"zstdcat {path} | ./Processing/cruncher/cruncher reddit-comments {output_path}")
            upload(f"processed/reddit/comments/{path.stem}.parquet")
            print(f"{path.stem} Uploaded.")


    # Upload any not uploaded files 
    print("Uploading missing files...")
    upload_all("processed/reddit/comments", overwrite=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 Scripts/process_reddit.py <submissions/comments/all>")
        sys.exit(1)
    processing_choice = sys.argv[1]
    if processing_choice == "all":
        print("Processing everything")
        process_reddit_all()
    elif processing_choice == 'submissions':
        print("Processing just submissions")
        process_reddit_submissions()
    elif processing_choice == 'comments':
        print("Processing just comments")
        process_reddit_comments()\
    


    

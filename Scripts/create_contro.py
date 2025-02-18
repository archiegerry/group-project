from s3 import *
import sys, os
from tqdm import tqdm
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# NOT WORKING

def build_contro(tallies, ):
    # crap


def select_datatype():
    contro = pd.DataFrame()
    # Get all the files from S3 (don't have the reddit tallies yet) 
    gnews_tally_paths = download_all("processed/news/tally")
    gnews_paths = download_all("processed/news/gnews_filtered")
    # Include when reddit tallies get added
    #reddit_tally_paths = download_all("processed/reddit/tally/")
    #reddit_paths = download_all("processed/news/gnews_filtered/")

if __name__ == "main":
    select_datatype()
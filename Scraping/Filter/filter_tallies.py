from s3 import *
import os
import pandas as pd
 
# NOT WORKING

def filter_tallies():   
    tally_paths = download_all("processed/news/tally", overwrite=False)
    tally = pd.read_parquet(s3_to_local_path(f"processed/news/tally/{path.stem}_tally.parquet"))

import pandas as pd 
import os
from tqdm import tqdm 
from dotenv import dotenv_values 
from pathlib import Path

BUCKET = "sentimentgroup"

# Local caching path
LOCAL_PATH = Path.home() / "s3local" / BUCKET
ARTIFACT_PATH = Path.home() / "artifacts" / BUCKET

# Get local path for a file (when saving before upload)
def s3_to_local_path(path):
    return LOCAL_PATH / path

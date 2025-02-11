import argparse
import json
import pandas as pd
from joblib import Parallel, delayed
from glob import glob
import os
from tqdm import tqdm
import boto3
from dotenv import dotenv_values
import bz2
import pickle
import importlib
import sys
from pathlib import Path


# All S3 processing is designed to be idempotent
# I.e. data should be written once and not updated
# Therefore caching is very easy

# S3
BUCKET = "sentimentgroup"

# Parse env
file_path = Path(__file__).parent.resolve()
config = dotenv_values(file_path / ".env")

# S3 Stuff
s3 = boto3.client(
    "s3",
    aws_access_key_id=config["S3_KEY"],
    aws_secret_access_key=config["S3_SECRET"],
    region_name=config["S3_REGION"],
) if "S3_KEY" in config else None

# Local caching path
LOCAL_PATH = Path.home() / "s3local" / BUCKET
ARTIFACT_PATH = Path.home() / "artifacts" / BUCKET

# Get local path for a file (when saving before upload)
def s3_to_local_path(path):
    return LOCAL_PATH / path

def artifact(path):
    return ARTIFACT_PATH / path

# Download a file to an equivalent local path
def download(s3_path, overwrite=False):
    # Ensure the local directory exists
    local_file_path = s3_to_local_path(s3_path)
    local_dir = local_file_path.parent
    os.makedirs(local_dir, exist_ok=True)  # Create directories if they don't exist

    # Check if the file already exists locally
    if local_file_path.is_file() and not overwrite:
        return

    # Download the file from S3
    s3.download_file(BUCKET, s3_path, str(local_file_path))

# Also note that uploading should be done by first writing to local dir then uploading
def upload(s3_path):
    local_file_path = s3_to_local_path(s3_path)
    if not local_file_path.is_file():
        raise Exception("Error Uploading to S3, expected file in: " + str(local_file_path))

    s3.upload_file(local_file_path, BUCKET, str(s3_path))

# Move, has to copy and delete :(
def move_s3_object(old_key, new_key):
    # Copy the object to a new key
    s3.copy(
        Bucket=BUCKET,
        CopySource={'Bucket': BUCKET, 'Key': old_key},
        Key=new_key
    )

    # Delete the original object
    s3.delete_object(Bucket=BUCKET, Key=old_key)

# List all paths for a given prefix
def list(s3_prefix):
    paginator = s3.get_paginator("list_objects")
    paths = []
    for result in paginator.paginate(Bucket=BUCKET, Prefix=s3_prefix):
        for obj in result.get("Contents", []):
            paths.append(obj["Key"])
    return paths

# Download everything for a given prefix
def download_all(s3_prefix, overwrite=False):
    paths = list(s3_prefix)
    for path in tqdm(paths):
        try:
            download(path, overwrite=overwrite)
        except:
            continue
    return paths

# Upload everything for a given prefix
def upload_all(s3_prefix, overwrite=True):
    skip = []
    if not overwrite:
        skip = [str(p) for p in list(s3_prefix)]

    local_prefix = s3_to_local_path(s3_prefix) 
    for local_path in tqdm(local_prefix.rglob("*")):
        path = str(local_path).replace(str(LOCAL_PATH) + "/", "")
        if path not in skip:
            if local_path.is_file():
                upload(path)

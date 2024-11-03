# group-project
Code repository for our COMP5530M Group Project

## Setup
1. Ensure some version (ideally 3.12+) of Python3 is installed.
2. Install packages from the requirements file:
```
python3 -m pip install -r requirements.txt
```
(You will likely have to create a venv which this step should be done in)

3. Put the .env file into the Scripts directory.

(Ensure the '.' preceding 'env' is there when downloaded from Google Drive as Chrome removes it by default)

## Scripts
Run all scripts from the main directory, i.e. python3 Scripts/script_name.py


### Reddit processing

0. Ensure Go is installed, cd into Scraping/Cruncher/cruncher and run go build

1. Use a torrent client to download some files from the data dump. (https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10)

2. Find the path of the downloaded *reddit* folder, e.g. ~/Downloads/reddit, and run the following to uplaod the raw files to S3:
```
python3 Scripts/upload_reddit_raw.py ~/Downloads/reddit
```

3. Run the processing script:
```
python3 Scripts/process_reddit.py
```
(If you are done with the raw files they can be deleted, I'll write a script to redownload them later in case we need to re-process).

### Using Reddit data
1. Download the processed data:
```
python3 Scripts/download_reddit_processed.py
```
2. Refer to Sentiment Analysis/example.ipynb

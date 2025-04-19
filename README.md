# Modelling the Impact of Online Public Sentiment on Stock Returns
Code repository for our COMP5530M Group Project. A modular pipeline for gathering data, processing data, performing sentiment analysis, and different forms of statistical analysis to uncover relationships between public sentiment and stock trends.

## Process Overview

1. **Data Retrieval:** Collects raw data from the web and APIs. News data from the GNews API, Reddit data from XXXX, and stock data from XXXX.

2. **Data Processing:** Transforms raw news and reddit files into structured `.parquet` datasets.

3. **Data Splitting:** Segments the processed data by mentioned companies (tickers), producing per-ticker artifacts to allow for future analysis.

4. **Sentiment Analysis:** Applies a RoBERTa sentiment model to generate sentiment scores for each text document.

5. **Statistical Analysis:** Analysis sentiment vs. stock performance, looking into correlations, and taking into account time delays and general stock market health.


## Setup
1. Ensure some version (ideally 3.12+) of Python3 is installed.
2. *(Optional but recommended)* Create and activate a virtual environment:
    
    `python -m venv /path/to/new/virtual/environment`
    
    `source <venv>/bin/activate`

3. Install packages from the requirements file: 

    `python3 -m pip install -r requirements.txt`

4. If working with S3, put the `.env` file into the Scripts directory.

**Each module has a corresponding `README.md` file with specific setup and usage instructions.**

---
---
---
---
---
---
## Example Pipeline
While parts of this pipeline can be automated, the end-to-end flow involves manual steps due to varying data formats, processing requirements, and storage setups. However, the Sentiment Analysis and Statistical Analysis modules are fully automatable — see the example notebooks in their respective folders for reproducible pipelines.

The process below outlines the steps we used to generate our results using AWS S3 for storage. Thanks to the modular structure, it can be easily adapted to different setups. Many scripts include flags or modes to enable/disable S3 integration, and as long as data matches the expected format, the sentiment and statistical analysis stages can be used independently.

### 1. Data Retrieval

#### 1.1 News Data:
Scrape and upload data to S3:

`python3 DataRetrieval/NewsReddit/gnews_download.py`

#### 1.2 Reddit Data:
Use a torrent client to download some files from the data dump: (https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10)

Upload downloaded data to S3:

`python3 Scripts/upload_reddit_raw.py ~/Downloads/reddit`

#### 1.3 Stock Data 
Historical stock data is pre-collected and available at: `DataRetrieval/Stocks/data/`. See `DataRetrieval/Stocks/README.md` for details on how it was gathered.

### 2. Process Reddit/News Files
Convert raw files into structured `.parquet` format.
```
python3 Scripts/process_news.py
python3 Scripts/process_reddit.py all
```
### 3. Split into Company-Specific Artifacts
Divide posts/articles and comments into per-ticker subsets for analysis.
```
python3 Scripts/split_news.py
python3 Scripts/split_reddit.py
```
### 4. Run Sentiment Analysis Pipeline
Use the notebook pipeline in `SentimentAnalysis/` to apply RoBERTa sentiment analysis.

### 5. Aggregate Sentiment per Company by Date
Create daily sentiment scores per company.
 ```
python3 Scripts/dataset_sort.py news-all
python3 Scripts/dataset_sort.py reddit-submissions-all
python3 Scripts/dataset_sort.py reddit-comments-all
 ```
### 6. Join All Datasets 
Merge the daily sentiment data for news and reddit, with daily stock prices and SP500 index prices.
```
python3 Scripts/dataset_join.py
```
### 7. Statistical Analysis
The `StatisticalAnalysis/` folder contains Jupyter Notebook pipelines for this process.
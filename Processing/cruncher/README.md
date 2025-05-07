# Cruncher

A Go module for processing and splitting large datasets of GNews articles and Reddit content. 

**Note:** There are python scripts in the `Scripts/` folder which automate this process for large numbers of files.

## Go Setup
#### Installing Go for WSL:

Instructions to install Go on WSL can be found [here](https://dev.to/deadwin19/how-to-install-golang-on-wslwsl2-2880).

#### Building the Application
Inside the `cruncher` directory: `go build`

This will produce a `cruncher` executable.

---
## Processing
### Description
Converts JSONL files (can be raw `.zst` compressed JSONL) of Reddit or GNews data into Parquet files with only the required columns.

### Usage
JSONL files:

`cat <input_file.json> | ./cruncher <mode> <output_file.parquet>`

`.zst` compressed JSONL files:

`zstdcat <input_file.zst> | ./cruncher <mode> <output_file.parquet>`


Supported modes:
- `reddit-submissions`
- `reddit-comments`
- `news-articles`


---
## Segmentation
### Description
Cruncher can also split processed Parquet files (Reddit or GNews) into multiple files by search terms, using a predefined mapping from:
`../../DataRetrieval/Stocks/data/search_terms_reduced.csv`

Each output file corresponds to a matched search term, helping isolate content related to specific stocks or entities.

### Usage
`./cruncher split-[news|reddit] <input.parquet> <output_folder/>`

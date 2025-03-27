from s3 import *
import sys, os
from tqdm import tqdm
import pandas as pd
import numpy as np
from symbol_to_filename import mappings
import re

## Check to see if any of the company filenames do not map to a stock ticker in 'symbol_to_filename'
def symbol_checker():
    inv_map = {v: k for k, v in mappings.items()}

    paths = s3_to_local_path('processed/news/contro/').glob("*.parquet")
    
    for path in paths:
        clean_filename = re.sub('_tally', '', path.stem)
        cleaner_filename = re.sub('_contro', '', clean_filename)
        print(inv_map[cleaner_filename])

# Provide all of the temp textstream filepaths to open based on the tally charts for each article 
def contro_tokeniser(articlepath):
    tally = pd.read_parquet(s3_to_local_path(f'processed/news/contro/tally/{articlepath.stem}_tally.parquet'))

    nonzero_columns = tally.columns[(tally != 0).any()]
    print(nonzero_columns.to_list())

    print(f'Hope arg1:{str(articlepath)}')
    #print(f'Hope arg2 list:{str(   )}')

def main():

    # Open articles to make appending _tally easier
    paths = s3_to_local_path('processed/news/contro/articles/').glob("*.parquet")

    # Tokenise all of the controversial articles
    for path in paths:
        contro_tokeniser(path)

    # Then combine all of the JSON into each processed/news/gnews_filtered article
        
   

if __name__ == "__main__":
    main()
from s3 import *
import sys, os
from tqdm import tqdm
import pandas as pd
import numpy as np
from symbol_to_filename import mappings
import re

## Check to see if any of the company filenames do not map to a stock ticker in 'symbol_to_filename'

def main():
    inv_map = {v: k for k, v in mappings.items()}

    paths = s3_to_local_path('processed/news/contro/').glob("*.parquet")
    
    for path in paths:
        clean_filename = re.sub('_tally', '', path.stem)
        cleaner_filename = re.sub('_contro', '', clean_filename)
        print(inv_map[cleaner_filename])

   

if __name__ == "__main__":
    main()
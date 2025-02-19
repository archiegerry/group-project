import pandas as pd
import nltk
import json
import os
import sys
from nltk.tokenize import sent_tokenize

nltk.download('punkt')
from s3 import *


# read in stock tickers from csv file
# TODO: do we want dict or df?
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    tickers_df['search_terms'] = tickers_df['search_terms'].str.split('|')
    return tickers_df.set_index('ticker')['search_terms'].to_dict()  # {'AAPL': ['Apple', 'AAPL', 'Mac'], ...}


# read in ticker-to-filename mapping
def load_mapping(mapping_file):
    with open(mapping_file, 'r', encoding='utf-8') as f:
        return json.load(f)  # eg: {"AAPL": "apple.json", "MSFT": "microsoft.json"}


# opens or creates list of json files
# TODO: do we open with write/append instead?
def open_temps(temp_list):
    temp_files = {}
    for temp in temp_list:
        if os.path.exists(temp):
            with open(temp, 'r', encoding='utf-8') as f:
                temp_files[temp] = json.load(f)
        else:
            temp_files[temp] = []
    return temp_files


# gets text string depending on format choice
def get_text(df, formatting):
    if formatting == "news":
        df['text'] = df['title'] + " " + df['description'] + " " + df['body']
    elif formatting == "reddit-post":
        df['text'] = df['title'] + " " + df['body']
    elif formatting == "reddit-comment":
        df['text'] = df['body']        
    else:
        print("Requires a format string of news, reddit-post, or reddit-comment")
        sys.exit(1)
    return df


# tokenise text to sentences
def sentence_split(df):
    df['sentences'] = df['text'].apply(sent_tokenize)

# get list of word indices of where any search terms have been mentioned, return list of indices and companies
def index(df, tickers):
    pass

# if any sentence does not mention a company, append it to previous sentence, return list of sentences
def combine_sentences():
    pass
    
# take index list and remove any instances where a stock is mentioned a second time in a row, return reduced index list
def reduce_index():
    pass

# if more than 1 stock is mentioned in a sentence, split the sentence into multiple sentences, return list of sentences
def split_sentences():
    pass


# every sentence now should have 1 indice: stock mentioned, concatenate all sentences that refer to the same company
# return original text split into x number of texts based on how many companies mentioned
def split_text(df, indices):
    pass


# TODO: before writing to file have to remove the text field from df and add it to body
    
    
def sentence_bucketiser(contro_file, temp_list, formatting, stock):
    
    # read in data
    try:
        df = pd.read_parquet(contro_file)
    except:
        print(f"Couldn't process {contro_file}")
        sys.exit(1)
    
    # get right text depending on formatting   
    df = get_text(df, formatting)
    
    # read in tickers
    tickers = read_tickers(stock)
    
    # open all files in temp list (json files )
    open_temps(temp_list)
    
    
    
    


#################################################
# Below if ran from command line
def main():
    # check for 2 command line arguments
    if len(sys.argv) != 5:
        print("Usage: python split_text.py <file> <formatting> <stock_file>")
        sys.exit(1)
        
    contro_file = sys.argv[1]
    formatting = sys.argv[2]
    tally_file = sys.argv[3]
    stock = sys.argv[4] 
    sentence_bucketiser(contro_file, tally_file, formatting, stock)

if __name__ == "__main__":
    main()
    
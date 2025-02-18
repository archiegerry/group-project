import pandas as pd
import sys
from s3 import *




def main():

    contro_file = sys.argv[1]
    formatting = sys.argv[2]
    
    try:
        df = pd.read_parquet(contro_file)
    except:
        print(f"Couldn't process {contro_file}")
        print("Usage: python split_text.py <contro_file> <news/reddit-post/reddit-comment>")
        sys.exit(1)
        
    if formatting == "news":
        df['text'] = df['title'] + " " + df['description'] + " " + df['body']
    elif formatting == "reddit-post":
        df['text'] = df['title'] + " " + df['body']
    elif formatting == "reddit-comment":
        df['text'] = df['body']        
    else:
        print("Usage: python tally.py <stock_file> <news/reddit/news-all/reddit-all> <input_file_optional> <output_file_optional>")
        sys.exit(1)


if __name__ == "__main__":
    main()
    
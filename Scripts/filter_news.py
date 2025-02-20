from s3 import *
import os
import pandas as pd
 
 
# get symbol from filename
def get_symbol(filename, tickers):
    # get company name
    company_name = str(filename).replace("_", " ").replace(".parquet", "").strip()
    for _, row in tickers.iterrows():
        search_terms_list = row['search_terms'].split('|')  # Convert '|' separated string into a list
        
        if company_name in search_terms_list:  # Check if company name exists in search terms
            return row['symbol']  # Return the matching symbol
    print("No search term found for company name:", company_name)
    return None  # Return None if


def main():
    # run below line if you dont have local gnews filtered folder
    s3_to_local_path("processed/news/tally_filtered").mkdir(parents=True, exist_ok=True)
    # s3_to_local_path("processed/news/gnews_filtered").mkdir(parents=True, exist_ok=True)
    # s3_to_local_path("processed/news/gnews").mkdir(parents=True, exist_ok=True)

    # get updated S3 files
    #download_all("processed/news", overwrite=False)

    submission_paths = s3_to_local_path("processed/news/gnews/").glob("*.parquet")

    print(str(submission_paths))

    # get search terms from csv
    tickers = pd.read_csv("filter_terms.csv")
    
    # go through every file in gnews
    for path in tqdm(submission_paths):
        # output path is same name in filtered folder
        output_path = s3_to_local_path(f"processed/news/gnews_filtered/{path.stem}.parquet")
        # skip if already processed
        if output_path.is_file():
            continue
            
        # get the corresponding symbol for the file.
        symbol = get_symbol(path.stem, tickers)
        # get news articles
        news = pd.read_parquet(path)
        # get tallies
        tally = pd.read_parquet(s3_to_local_path(f"processed/news/tally/{path.stem}_tally.parquet"))

        print(str(tally.head))
        
        # filter news articles
        if symbol in tally.columns:
            valid_rows = tally[symbol] >= 3

            # rows where no other symbol has count > 2x the current symbol count
            other_symbols = [col for col in tally.columns if col != symbol]
            for other_symbol in other_symbols:
                valid_rows &= ~(tally[other_symbol] > tally[symbol])

            # remove tally row as well
            tally = tally[valid_rows]
            
            # apply filtering to news
            news = news[valid_rows]
        else:
            print(f"Symbol {symbol} not found in tally file for {path.stem}. Skipping.")
            continue
        
        # Save filtered news
        news.to_parquet(output_path, index=False)
        tally.to_parquet(s3_to_local_path(f"processed/news/tally_filtered/{path.stem}_tally.parquet"), index=False)
        print(f"Saved filtered file: {output_path}")



if __name__ == "__main__":
    
    # upload_all("processed/news/gnews_filtered", overwrite=False)
    main()
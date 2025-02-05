import pandas as pd
import re
import sys


# read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    tickers = tickers_df[['symbol', 'security', 'search_terms']]
    return tickers


# read in reddit submission parquet file as dataframe
def read_reddit_parq(path):
    df = pd.read_parquet(path)
    df['text'] = df['title'].fillna('') + " " + df['body'].fillna('')  
    return df['text'].tolist()


# read in news article parquet file as dataframe
def read_news_parq(path):
    df = pd.read_parquet(path)
    df['text'] = df['title'].fillna('') + " " +df['description'].fillna('') + " " + df['body'].fillna('') 
    return df['text'].tolist()


# write dataframe to parquet file
def write_parq(df, path):
    df.to_parquet(path)
    return


# tally mentions of each stock ticker in reddit posts
def count_tickers(texts, tickers):
    # list of counts
    counts_list = []
    
    for text in texts:
        text_lower = text.lower()
        
        # set counts to 0 for all tickers
        ticker_counts = {ticker: 0 for ticker in tickers['symbol']}
        
        # iterate through each ticker
        for _, row in tickers.iterrows():
            total_count = 0
            symbol, security, search_terms = row['symbol'], row['security'], row['search_terms']

            # Check for security name
            if security and pd.notna(security):
                total_count += len(re.findall(rf'\b{re.escape(security.lower())}\b', text_lower))
                                
            # Check for additional search terms
            if search_terms and pd.notna(search_terms):
                terms_list = search_terms.lower().split('/')  # Convert to lowercase & split
                for term in terms_list:
                    if term.strip():  
                        total_count += len(re.findall(rf'\b{re.escape(term.strip())}\b', text_lower))

            ticker_counts[symbol] = total_count
        
        counts_list.append(ticker_counts)
        
    counts_df = pd.DataFrame(counts_list)
    return counts_df
            


def main():
    if len(sys.argv) != 5:
        print("Usage: python tally.py <stock_file> <news/reddit> <input_file> <output_file>")
        sys.exit(1)
    
    stock_file = sys.argv[1]
    input_file = sys.argv[3]
    output_file = sys.argv[4]
    
    # read in stock tickers
    tickers = read_tickers(stock_file)
   
    # read in news articles or reddit submissions depending on cmd line arg
    if sys.argv[2] == "news":
        texts = read_news_parq(input_file)
    elif sys.argv[2] == "reddit":
        texts = read_reddit_parq(input_file)
    else:
        print("Usage: python tally.py <stock_file> <news/reddit> <input_file> <output_file>")
        sys.exit(1)
    
    # count mentions of each stock ticker in news articles
    counts = count_tickers(texts, tickers)
    
    # write counts to parquet file
    write_parq(counts, output_file)
    
    


if __name__ == "__main__":
    main()
    
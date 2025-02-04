import pandas as pd


# read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    tickers = tickers_df['symbol', 'security', 'search_terms']
    return tickers


# read in reddit submission parquet file as dataframe
def read_reddit_parq(path):
    df = pd.read_parquet(path)
    return df

# read in news article parquet file as dataframe
def read_news_parq(path):
    df = pd.read_parquet(path)
    return df


# tally mentions of each stock ticker in reddit posts
def count_tickers_reddit(df, tickers):
    # create df to count mentions of each stock ticker
    ticker_cols = {ticker: 0 for ticker in tickers['symbol']}
    counts_df = pd.DataFrame(columns=tickers['symbol'])
    
    # iterate through news articles
    for i, row in df.iterrows():
        # iterate through stock tickers
        for ticker in ticker_cols:
            # count mentions of ticker in article
            count = row['text'].count(ticker)
            ticker_cols[ticker] += count
    return


# write dataframe to parquet file
def write_parq(df, path):
    df.to_parquet(path)
    return



def main():
    # read in stock tickers
    tickers = read_tickers("../stock_list.csv")
    # read in news articles
    news = read_parq("input.parquet")
    # count mentions of each stock ticker in news articles
    counts = count_tickers(news, tickers)
    # write counts to parquet file
    write_parq(counts, "output.parquet")
from s3 import s3_to_local_path, download_all, upload
import pandas as pd
from matplotlib import pyplot as plt
from datetime import timedelta

"""
Big old join, produces a table with columns:
- date
- symbol
- ft_submissions_{subreddit}
- ft_comments_{subreddit}
- ft_news_{domain}

Plus price data
- close, low, high, open

We also roll sentiment data into tomorrows date, meaning we can trade at *current day* close instead of lagging by 1 day.
"""

def load_comments():
    comments = s3_to_local_path("processed/reddit/comments_twitter_roberta/")
    df = pd.concat([
        pd.read_parquet(path).assign(symbol=path.stem) for path in comments.glob("*.parquet")
    ])
    df['date'] = pd.to_datetime(df.datetime, unit='ms')
    df['date'] = df.date.add(timedelta(hours=5)).dt.date # Move late posts into tomorrows data
    df['weighted_roberta'] = df.roberta_normalised_compound * df.score
    posts = df.groupby('post_id').agg({
        'date': 'last',
        'subreddit': 'last',
        'symbol': 'last',
        'score': 'last',
        'weighted_roberta': 'last',
    })
    days = posts.groupby(['date', 'symbol', 'subreddit']).agg({
        'score': 'sum',
        'weighted_roberta': 'sum',
    }).reset_index()
    days['avg_sentiment'] = days.weighted_roberta / days.score
    subs = days.subreddit.unique()
    symbols = days.symbol.unique()
    # New dataframe with symbol and date index
    ndf = pd.concat([
        pd.DataFrame({
            'date': pd.date_range(days.date.min(), days.date.max()),
            'symbol': symbol,
        }) for symbol in symbols
    ])
    ndf['date'] = ndf.date.dt.date
    for sub in subs:
        ndf = ndf.merge(days[days.subreddit == sub][['date', 'symbol', 'avg_sentiment']].rename(columns={'avg_sentiment': 'ft_comments_' + sub}), on=['date', 'symbol'], how='left')
    return ndf.rename(columns={'datetime': 'dr'})
    


def load_submissions():
    submissions = s3_to_local_path("processed/reddit/submissions_twitter_roberta/")
    df = pd.concat([
        pd.read_parquet(path).assign(symbol=path.stem) for path in submissions.glob("*.parquet")
    ])
    df['date'] = pd.to_datetime(df.datetime, unit='ms')
    df['date'] = df.date.add(timedelta(hours=5)).dt.date # Move late posts into tomorrows data
    df['weighted_roberta'] = df.roberta_normalised_compound * df.score
    posts = df.groupby('post_id').agg({
        'date': 'last',
        'subreddit': 'last',
        'symbol': 'last',
        'score': 'last',
        'weighted_roberta': 'last',
    })
    days = posts.groupby(['date', 'symbol', 'subreddit']).agg({
        'score': 'sum',
        'weighted_roberta': 'sum',
    }).reset_index()
    days['avg_sentiment'] = days.weighted_roberta / days.score
    subs = days.subreddit.unique()
    symbols = days.symbol.unique()
    # New dataframe with symbol and date index
    ndf = pd.concat([
        pd.DataFrame({
            'date': pd.date_range(days.date.min(), days.date.max()),
            'symbol': symbol,
        }) for symbol in symbols
    ])
    ndf['date'] = ndf.date.dt.date
    for sub in subs:
        ndf = ndf.merge(days[days.subreddit == sub][['date', 'symbol', 'avg_sentiment']].rename(columns={'avg_sentiment': 'ft_submissions_' + sub}), on=['date', 'symbol'], how='left')
    return ndf.rename(columns={'datetime': 'dr'})

def load_news(num_sources=50):
    news = s3_to_local_path("processed/news/twitter_roberta/")
    df = pd.concat([
        pd.read_parquet(path).assign(symbol=path.stem) for path in news.glob("*.parquet")
    ])
    df = df.drop_duplicates(['url'], keep='last')
    df['date'] = pd.to_datetime(df.dt, unit='ms')
    df['date'] = df.date.add(timedelta(hours=5)).dt.date # Move late posts into tomorrows data
    df['domain'] = (
        df.domain.str.replace(' ', '_')
            .str.replace(',', '')
            .str.replace("'", '')
            .str.replace('.com', '')
            .str.replace('+', '')
            .str.replace('-', '_')
            .str.replace('/', '')
            .str.replace('!', '')
            .str.lower()
    )
    # Equally weighted for now
    df['weighted_roberta'] = df.roberta_normalised_compound
    df['weight'] = 1
    posts = df.groupby('url').agg({
        'date': 'last',
        'domain': 'last',
        'symbol': 'last',
        'weighted_roberta': 'last',
        'weight': 'last',
    })
    days = posts.groupby(['date', 'symbol', 'domain']).agg({
        'weighted_roberta': 'sum',
        'weight': 'sum',
    }).reset_index()
    days['avg_sentiment'] = days.weighted_roberta / days.weight
    
    # We cant use all news sources, select those with the best coverage (articles over most days)
    t = df.drop_duplicates(['date', 'domain'], keep='last')
    # Group rest into 'other'?
    sources = t.domain.value_counts().sort_values()[-num_sources:].reset_index().domain.to_list()
    symbols = days.symbol.unique()

    # New dataframe with symbol and date index
    ndf = pd.concat([
        pd.DataFrame({
            'date': pd.date_range(days.date.min(), days.date.max()),
            'symbol': symbol,
        }) for symbol in symbols
    ])
    ndf['date'] = ndf.date.dt.date
    for source in sources:
        ndf = ndf.merge(days[days.domain == source][['date', 'symbol', 'avg_sentiment']].rename(columns={'avg_sentiment': 'ft_news_' + source}), on=['date', 'symbol'], how='left')
    return ndf



def join_datasets_with_sources():
    comments_df = load_comments()
    submissions_df = load_submissions()
    news_df = load_news()
    prices = pd.read_parquet(s3_to_local_path("marketdata/daily_prices.parquet"))
    sp500_prices = pd.read_parquet(s3_to_local_path("marketdata/sp500_daily_prices.parquet"))

    # Get all symbols across datasets
    symbols = []
    symbols += list(comments_df.symbol.unique())
    symbols += list(submissions_df.symbol.unique())
    symbols += list(news_df.symbol.unique())
    symbols = list(set(symbols))
    start_date = min(comments_df.date.min(), submissions_df.date.min(), news_df.date.min())
    end_date = max(comments_df.date.max(), submissions_df.date.max(), news_df.date.max())

    # New dataframe with symbol and date index
    df = pd.concat([
        pd.DataFrame({
            'date': pd.date_range(start_date, end_date),
            'symbol': symbol,
        }) for symbol in symbols
    ])
    df['date'] = df.date.dt.date

    # Join columns
    comments_columns = [c for c in comments_df.columns if c.startswith("ft_")]
    submissions_columns = [c for c in submissions_df.columns if c.startswith("ft_")]
    news_columns = [c for c in news_df.columns if c.startswith("ft_")]
    df = df.merge(comments_df[["symbol", "date"] + comments_columns], how="left", on=["symbol", "date"])
    df = df.merge(submissions_df[["symbol", "date"] + submissions_columns], how="left", on=["symbol", "date"])
    df = df.merge(news_df[["symbol", "date"] + news_columns], how="left", on=["symbol", "date"])

    # Prices
    prices["date"] = prices.date.dt.date.astype(str)
    df = df.merge(prices[["symbol", "date", "open", "close", "high", "low"]], how="left", on=["symbol", "date"])

    # Index prices
    sp500_prices["Date"] = pd.to_datetime(sp500_prices.Date, dayfirst=True)
    sp500_prices["Date"] = sp500_prices.Date.dt.date.astype(str)
    sp500_prices["Open"] = sp500_prices.Open.str.replace(",", "").astype(float)
    sp500_prices["Price"] = sp500_prices.Price.str.replace(",", "").astype(float)
    df = df.merge(sp500_prices[["Date", "Open", "Price"]].rename(columns={
        "Date": "date", "Open": "sp500_open", "Price": "sp500_close",
    }), how="left", on=["date"])
    return df.rename(columns={'date': 'dt'})

if __name__ == "__main__":
    # Ensure we have all required data
    s3_to_local_path("processed/news/twitter_roberta/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/submissions_twitter_roberta/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("processed/reddit/comments_twitter_roberta/").mkdir(parents=True, exist_ok=True)
    s3_to_local_path("marketdata/").mkdir(parents=True, exist_ok=True)
    download_all("processed/news/twitter_roberta/", overwrite=False)
    download_all("processed/reddit/submissions_twitter_roberta/", overwrite=False)
    download_all("processed/reddit/comments_twitter_roberta/", overwrite=False)
    download_all("marketdata/", overwrite=False)

    # Join and upload
    local_path = s3_to_local_path("datasets/roberta_sources.parquet")
    df = join_datasets_with_sources()
    df.to_parquet(local_path)
    upload(local_path)
    
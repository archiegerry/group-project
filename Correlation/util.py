import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from datetime import datetime
from scipy.stats import pearsonr
"""
Loads our dataset and adds basic features (ft_) and targets (tgt_) with some guarantees.

TODO - add more features
TODO - add volatility targets

"""
def load_dataset(
    path,    
    # Target returns horizons to add in days
    target_horizons = [30],
    # Lookback horizons for calculating historical rolling averages
    lookback_periods = [30],
    # Enable residualised returns (vs sp500) as a target (takes a little while)
    residualise_returns = True,
    # Restrict date range
    start_date = None,
    end_date = None,
):
    df = pd.read_parquet(path)
    df["dt"] = pd.to_datetime(df.dt)

    # By user
    start_date = pd.to_datetime(start_date) if start_date is not None else datetime(1000, 1, 1)
    end_date = pd.to_datetime(end_date) if end_date is not None else datetime(3000, 1, 1)
    df = df[(df.dt >= start_date) & (df.dt <= end_date)].copy()
    
    # Find start and end of price data
    start_date_prices = df[~df.open.isna()].dt.min()
    end_date_prices = df[~df.open.isna()].dt.max()
    df = df[(df.dt >= start_date_prices) & (df.dt <= end_date_prices)].copy()


    # Backfill prices - we can trade at a later price, but not an earlier one
    df["open"] = df.open.bfill()
    df["close"] = df.close.bfill()
    df["high"] = df.high.bfill()
    df["low"] = df.low.bfill()
    df["sp500_open"] = df.sp500_open.bfill()
    df["sp500_close"] = df.sp500_close.bfill()

    # Tradeable price today is tomorrows close, we could use open but close is a better price in less liquid stocks
    # We use tomorrow to avoid data leakage when an article comes out after close on a day
    df.sort_values(["symbol", "dt"], inplace=True)
    df["tradeable_price"] = df.groupby("symbol").close.shift(-1)
    df["sp_tradeable_price"] = df.groupby("symbol").sp500_close.shift(-1)

    # 1 day forward returns
    df["forward_returns_1"] = df.groupby("symbol")["tradeable_price"].pct_change(-1)

    # Add future returns
    if residualise_returns:
        print("Adding future returns (residualizing takes ~60s, so 60s * number of returns added)")
    
    for horizon in tqdm(target_horizons):
        df[f"forward_close_{horizon}"] = df.groupby("symbol").close.shift(-horizon)
        df[f"forward_close_{horizon}"] = df.groupby("symbol")[f"forward_close_{horizon}"].ffill()
        df[f"tgt_forward_returns_{horizon}"] = np.log(df[f"forward_close_{horizon}"] / df["tradeable_price"])
        df[f"tgt_forward_returns_{horizon}"] = df[f"tgt_forward_returns_{horizon}"].fillna(0)
        df.drop(columns=f"forward_close_{horizon}", inplace=True)

        # Add volatility targets (shifts back horizon days and calculates std of returns over horizon period)
        df[f"vol_tgt_{horizon}"] = df.groupby("symbol")["forward_returns_1"].shift(-horizon).rolling(window=horizon).std().reset_index(0, drop=True)
        df[f"vol_tgt_{horizon}"] = df[f"vol_tgt_{horizon}"].ffill()

        # Add future returns for SP500 as well, for normalizing
        df[f"sp_forward_close_{horizon}"] = df.groupby("symbol").sp500_close.shift(-horizon)
        df[f"sp_forward_close_{horizon}"] = df.groupby("symbol")[f"sp_forward_close_{horizon}"].ffill()
        df[f"sp_forward_returns_{horizon}"] = np.log(df[f"sp_forward_close_{horizon}"] / df["sp_tradeable_price"])
        df[f"sp_forward_returns_{horizon}"] = df[f"sp_forward_returns_{horizon}"].fillna(0)
        df.drop(columns=f"sp_forward_close_{horizon}", inplace=True)

        if residualise_returns:
            # Residualise vs sp returns
            df[f"tgt_resid_returns_{horizon}"] = 0.0
            for symbol in df.symbol.unique():
                sdf = df[df.symbol == symbol].copy()
                m = sdf[[f"sp_forward_returns_{horizon}", f"tgt_forward_returns_{horizon}"]].values.T
                beta = np.cov(m)[0,1] / (np.sqrt(np.var(m[0]) * np.var(m[1])))
                beta = np.nan_to_num(beta, neginf=0, posinf=0)
                df.loc[df.symbol == symbol, f"tgt_resid_returns_{horizon}"] = sdf[f"tgt_forward_returns_{horizon}"] - beta*sdf[f"sp_forward_returns_{horizon}"]

    # Fillna (features)
    df["ft_news"] = df.news_sentiment.fillna(0)
    df["ft_submissions"] = df.submissions_sentiment.fillna(0)
    df["ft_comments"] = df.comments_sentiment.fillna(0)
    df["ft_combined"] = df.ft_news + df.ft_submissions + df.ft_comments
    # Abs features (in future could look at number of posts instead)
    df["ft_abs_news"] = df.news_sentiment.fillna(0).abs()
    df["ft_abs_submissions"] = df.submissions_sentiment.fillna(0).abs()
    df["ft_abs_comments"] = df.comments_sentiment.fillna(0).abs()
    df["ft_combined_abs"] = df.ft_abs_news + df.ft_abs_submissions + df.ft_abs_comments


    # Add rolling features and volatility of features (look at last N days)
    fts = [c for c in df.columns if c.startswith("ft_")]
    for lookback in lookback_periods:
        for col in fts:
            df[f"{col}_{lookback}"] = df.groupby("symbol")[col].rolling(lookback, min_periods=0).mean().reset_index(0, drop=True)
            df[f"vol_{col}_{lookback}"] = df.groupby("symbol")[col].rolling(lookback, min_periods=0).std().reset_index(0, drop=True)

    return df

# Helper function for CS vs Feature plot
def csvf_plot(df, ft, tgt):
    cs = df.sort_values(ft)
    cs["cs_ret"] = cs[tgt].cumsum()
    cs["cs_trades"] = 1
    cs["cs_trades"] = cs.cs_trades.cumsum()
    ax = cs.plot(x=ft, y="cs_ret")
    cs.plot(x=ft, y="cs_trades", ax=ax.twinx(), style='r--')
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from datetime import datetime
from scipy.stats import pearsonr
import seaborn as sns

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
    # If using the sources dataset we
    # a) don't need to wait a day to trade
    # b) don't need to add ft_ columns (they are already named)
    sources = False, 
    group_sources = False,
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
    df.sort_values(["symbol", "dt"], inplace=True)

    # For non-sources data set, tradeable price today is tomorrows close, 
    # we could use open but close is a better price in less liquid stocks
    # We use tomorrow to avoid data leakage when an article comes out after close on a day
    # For sources dataset this is taken care of
    if sources:
        df["tradeable_price"] = df.close
        df["sp_tradeable_price"] = df.sp500_close
    else:
        df["tradeable_price"] = df.groupby("symbol").close.shift(-1)
        df["sp_tradeable_price"] = df.groupby("symbol").sp500_close.shift(-1)

    # Add future returns
    if residualise_returns:
        print("Adding future returns (residualizing takes ~60s, so 60s * number of returns added)")
    
    for horizon in tqdm(target_horizons):
        df[f"forward_close_{horizon}"] = df.groupby("symbol").close.shift(-horizon)
        df[f"forward_close_{horizon}"] = df.groupby("symbol")[f"forward_close_{horizon}"].ffill()
        df[f"tgt_forward_returns_{horizon}"] = np.log(df[f"forward_close_{horizon}"] / df["tradeable_price"])
        df[f"tgt_forward_returns_{horizon}"] = df[f"tgt_forward_returns_{horizon}"].fillna(0)
        df.drop(columns=f"forward_close_{horizon}", inplace=True)

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


    if sources:
        # Just fillna 
        for c in df.columns:
            if c.startswith('ft_'):
                df[c] = df[c].fillna(0)

        # If grouping, construct average features (essentially the same as the base dataset but enables us to trade same-day)
        if group_sources:
            df["ft_news"] = sum([df[c] for c in df.columns if c.startswith("ft_news_")])
            df["ft_submissions"] = sum([df[c] for c in df.columns if c.startswith("ft_submissions_")])
            df["ft_comments"] = sum([df[c] for c in df.columns if c.startswith("ft_comments_")])
            for c in df.columns:
                if c.startswith("ft_news_") or c.startswith("ft_submissions_") or c.startswith("ft_comments_"):
                    del df[c]
    else:
        # Rename feature columns
        df["ft_news"] = df.news_sentiment.fillna(0)
        df["ft_submissions"] = df.submissions_sentiment.fillna(0)
        df["ft_comments"] = df.comments_sentiment.fillna(0)
        df["ft_combined"] = df.ft_news + df.ft_submissions + df.ft_comments
        # Abs features (in future could look at number of posts instead)
        df["ft_abs_news"] = df.news_sentiment.fillna(0).abs()
        df["ft_abs_submissions"] = df.submissions_sentiment.fillna(0).abs()
        df["ft_abs_comments"] = df.comments_sentiment.fillna(0).abs()
        df["ft_combined_abs"] = df.ft_abs_news + df.ft_abs_submissions + df.ft_abs_comments


    # Add rolling features (look at last N days)
    fts = [c for c in df.columns if c.startswith("ft_")]
    for lookback in lookback_periods:
        for col in fts:
            # Copy to prevent fragmentation
            df[f"{col}_{lookback}"] = np.array(((
                (df.groupby("symbol")[col].rolling(lookback, min_periods=0).mean().reset_index(0, drop=True)
                - df.groupby("symbol")[col].rolling(3*lookback, min_periods=0).mean().reset_index(0, drop=True))
            ) / df.groupby("symbol")[col].rolling(6*lookback, min_periods=0).std().reset_index(0, drop=True)).fillna(0))
            

    # Add normalised rolling features (feature normalised cross sectionally)
    # for lookback in lookback_periods:
    #     for col in fts:
    #         df[f"{col}_{lookback}n"] = (
    #             df[f"{col}_{lookback}"] #- df.groupby("dt")[f"{col}_{lookback}"].transform('mean')
    #         ) / df.groupby("dt")[f"{col}_{lookback}"].transform('std')

    return df.copy()

# Helper function for CS vs Feature plot
def csvf_plot(df, ft, tgt):
    cs = df.sort_values(ft)
    cs["cs_ret"] = cs[tgt].cumsum()
    cs["cs_trades"] = 1
    cs["cs_trades"] = cs.cs_trades.cumsum()
    ax = cs.plot(x=ft, y="cs_ret")
    cs.plot(x=ft, y="cs_trades", ax=ax.twinx(), style='r--')

# Plot a correlation heatmap
def plot_feature_target_correlation(df, exclude=[]):
    target_cols = [col for col in df.columns if col.startswith('tgt_')]
    feature_cols = [col for col in df.columns if col.startswith('ft_') and col not in exclude]
    corr_matrix = df[feature_cols + target_cols].corr()
    corr_submatrix = corr_matrix.loc[feature_cols, target_cols]

    # Plot the heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(corr_submatrix, annot=True, fmt=".2f", cmap="coolwarm", linewidths=0.5)
    plt.title("Feature vs Target Correlation Matrix")
    plt.xlabel("Targets")
    plt.ylabel("Features")
    plt.show()

    import pandas as pd
import numpy as np



# Simulate a trading portfolio with capital allocation based on signal values.
def simulate_portfolio_vectorized(df, initial_capital=10000.0, tc_rate=0.001):
    # Convert input dataframe to a pivot table format for vectorized operations
    # This creates a multi-indexed DataFrame with dates as index and symbols as columns
    pivot_close = df.pivot_table(index='dt', columns='symbol', values='close')
    pivot_signal = df.pivot_table(index='dt', columns='symbol', values='signal')
    
    # Fill NaN with 0 for signals (no signal = no allocation)
    pivot_signal = pivot_signal.fillna(0)
    
    # Get unique dates and symbols
    dates = pivot_close.index.unique()
    symbols = pivot_close.columns.unique()
    n_dates = len(dates)
    n_symbols = len(symbols)
    
    # Initialize arrays to store results
    portfolio_values = np.zeros(n_dates)
    cash_values = np.zeros(n_dates)
    position_values = np.zeros(n_dates)
    transaction_costs = np.zeros(n_dates)
    positions_array = np.zeros((n_dates, n_symbols))
    
    # Initial cash
    cash_values[0] = initial_capital
    portfolio_values[0] = initial_capital
    
    # Process each date (starting from the second date)
    for t in range(1, n_dates):
        prev_date = dates[t-1]
        curr_date = dates[t]
        
        # Get prices and signals for current date
        curr_prices = pivot_close.loc[curr_date].values
        curr_signals = pivot_signal.loc[curr_date].values
        
        # Replace NaN prices with previous day's prices or zeros
        prev_prices = pivot_close.loc[prev_date].values
        mask_curr_nan = np.isnan(curr_prices)
        curr_prices[mask_curr_nan] = prev_prices[mask_curr_nan]
        curr_prices[np.isnan(curr_prices)] = 0
        
        # Get previous positions
        prev_positions = positions_array[t-1]
        
        # Current position values before rebalancing
        prev_position_values = prev_positions * curr_prices
        prev_portfolio_value = cash_values[t-1] + np.sum(prev_position_values)
        
        # Calculate total signal and allocation ratios
        total_signal = np.sum(curr_signals)
        
        if total_signal > 0:
            # Calculate target positions based on signal allocation
            # allocation_ratios = np.exp(curr_signals) / total_signal
            allocation_ratios = curr_signals / total_signal
            target_values = prev_portfolio_value * allocation_ratios
            target_positions = np.zeros_like(target_values)
            
            # Avoid division by zero
            mask_valid_prices = curr_prices > 0
            target_positions[mask_valid_prices] = target_values[mask_valid_prices] / curr_prices[mask_valid_prices]
            
            # Calculate trades
            trades = target_positions - prev_positions
            trade_values = np.abs(trades) * curr_prices
            
            # Calculate transaction costs
            trade_costs = trade_values * tc_rate
            total_cost = np.sum(trade_costs)
            
            # Process buys (positive trades)
            buy_mask = trades > 0
            buy_values = trade_values[buy_mask]
            buy_costs = trade_costs[buy_mask]
            total_buy_cost = np.sum(buy_values) + np.sum(buy_costs)
            
            # Check if we have enough cash
            if total_buy_cost <= cash_values[t-1]:
                # Execute all trades
                new_positions = target_positions
                new_cash = cash_values[t-1] - np.sum(buy_values) - np.sum(buy_costs)
                
                # Add proceeds from sells
                sell_mask = trades < 0
                sell_values = trade_values[sell_mask]
                sell_costs = trade_costs[sell_mask]
                new_cash += np.sum(sell_values) - np.sum(sell_costs)
            else:
                # Not enough cash, scale down buys
                # This is a simplification; in reality, you might want to prioritize trades
                scale_factor = cash_values[t-1] / total_buy_cost if total_buy_cost > 0 else 0
                scaled_buys = trades.copy()
                scaled_buys[buy_mask] *= scale_factor
                
                # Execute scaled trades
                new_positions = prev_positions + scaled_buys
                
                # Calculate actual costs after scaling
                actual_buy_values = np.abs(scaled_buys[buy_mask]) * curr_prices[buy_mask]
                actual_buy_costs = actual_buy_values * tc_rate
                
                new_cash = cash_values[t-1] - np.sum(actual_buy_values) - np.sum(actual_buy_costs)
                
                # Process sells normally
                sell_mask = trades < 0
                sell_values = trade_values[sell_mask]
                sell_costs = trade_costs[sell_mask]
                new_cash += np.sum(sell_values) - np.sum(sell_costs)
                
                # Recalculate transaction costs
                total_cost = np.sum(actual_buy_costs) + np.sum(sell_costs)
        else:
            # No signals, keep positions the same
            new_positions = prev_positions
            new_cash = cash_values[t-1]
            total_cost = 0
        
        # Update state
        positions_array[t] = new_positions
        cash_values[t] = new_cash
        position_values[t] = np.sum(new_positions * curr_prices)
        portfolio_values[t] = cash_values[t] + position_values[t]
        transaction_costs[t] = total_cost
    
    # Create results DataFrame
    portfolio_df = pd.DataFrame({
        'date': dates,
        'portfolio_value': portfolio_values,
        'cash': cash_values,
        'positions_value': position_values,
        'transactions_cost': transaction_costs
    })
    
    # Add returns calculations
    portfolio_df['daily_return'] = (portfolio_df['portfolio_value'] / 
                                   portfolio_df['portfolio_value'].shift(1) - 1)
    portfolio_df['cumulative_return'] = (1 + portfolio_df['daily_return']).cumprod() - 1
    
    # Set date as index
    portfolio_df = portfolio_df.set_index('date')
    
    # Create positions DataFrame if needed
    positions_df = pd.DataFrame(positions_array, index=dates, columns=symbols)
    
    return portfolio_df, positions_df
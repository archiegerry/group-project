import requests
import pandas as pd
import numpy as np 
from tqdm import tqdm
from time import sleep

# Download price history for a symbol, convert to a pd dataframe
def download_symbol(symbol):
    req = requests.get(f"https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate=2014-12-27&limit=9999&todate=2024-12-27&random=20", headers={
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    })
    data = req.json()
    try:   
        rows = data["data"]["tradesTable"]["rows"]
    except:
        print(f"Unable to get data for symbol {symbol}, skipping")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], format="%m/%d/%Y")
    df["open"] = pd.to_numeric(df["open"].str.replace(",", "").str.replace("$", ""), errors='coerce')
    df["high"] = pd.to_numeric(df["high"].str.replace(",", "").str.replace("$", ""), errors='coerce')
    df["low"] = pd.to_numeric(df["low"].str.replace(",", "").str.replace("$", ""), errors='coerce')
    df["close"] = pd.to_numeric(df["close"].str.replace(",", "").str.replace("$", ""), errors='coerce')
    df["volume"] = pd.to_numeric(df["volume"].str.replace(",", ""), errors='coerce')
    df["symbol"] = symbol
    sleep(1)
    return df
 
# Download all symbols from 
def main():
    stock_list = pd.read_csv("Scraping/stock_list.csv")
    symbols = stock_list.symbol.unique()
    dfs = []
    for symbol in tqdm(symbols):
        df = download_symbol(symbol)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True).reset_index()
    df.to_parquet("Scraping/MarketData/daily_prices.parquet")
if __name__ == "__main__":
    main()
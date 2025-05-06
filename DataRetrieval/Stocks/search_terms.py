import pandas as pd
import re
import sys
import os


"""
Generates search terms for stock tickers from the stock_list, based on the stock symbol, security name, and any additional search terms provided.
Uses an ignore list for commonly used words.

USAGE:
    python search_terms.py <path_to_stock_list> <path_to_output_file>

    eg. python search_terms.py data/stock_list.csv data/search_terms.csv
"""

news_ignore = ['AFL', 'A', 'ARE', 'ALL', 'T', 'BA', 'C', 'COP', 'COO', 'DAY', 'DG', 'D', 
               'EL', 'FAST', 'F', 'IT', 'GM', 'HAS', 'HD', 'ICE', 'IP', 'J', 'K', 'L', 'MAS', 
               'MS', 'NI', 'NSC', 'ON', 'PH', 'PM', 'PSA', 'O', 'ROK', 'NOW', 'SW', 'USB', 'V']


# Read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    return tickers_df


# Generate search terms for each stock ticker
def generate_terms(symbol, security, search_terms):
    searches = []
    
    if symbol:
        if symbol not in news_ignore:
            # Add ticker variations and their plurals/possessives
            searches.append(symbol)
            searches.append(f'{symbol}s')
        searches.append(f"${symbol}")
        searches.append(f"${symbol}s")

    if security:
        searches.append(security) 
        searches.append(security.lower())
        searches.append(security.upper())
        if security[-1] != 's':
            searches.append(f"{security}s")
            searches.append(f"{security.lower()}s")
            searches.append(f"{security.upper()}S")
            
    if search_terms:
        if isinstance(search_terms, str):
            split_terms = search_terms.split('/')
            for term in split_terms:
                if term.strip():
                    searches.append(term.strip())
                    searches.append(term.strip().lower())
                    searches.append(term.strip().upper())
                    if term.strip()[-1] != 's':
                        searches.append(f"{term.strip()}s")
                        searches.append(f"{term.strip().lower()}s")
                        searches.append(f"{term.strip().upper()}S")

        searches = list(set(searches))
    return searches
    
# Write search terms to csv file
def write_csv(symbol, terms, path):
    df = pd.DataFrame({'symbol': symbol, 'search_terms': ['|'.join(term) for term in terms]})
    df.to_csv(path, index=False)
    return
    

def main():   
    if len(sys.argv) != 3:
        print("Usage: python search_terms.py <path_to_stock_list> <path_to_output_file>")
        sys.exit(1)
    if not os.path.exists(sys.argv[1]):
        print(f"File {sys.argv[1]} does not exist.")
        sys.exit(1)

    tickers = read_tickers(sys.argv[1])

    all_terms = []
    for _, row in tickers.iterrows():
        symbol = str(row['symbol']).strip() if pd.notna(row['symbol']) else ""
        security = str(row['security']).strip() if pd.notna(row['security']) else ""
        search_terms = str(row['search_terms']).strip() if pd.notna(row['search_terms']) else ""
        new_terms = generate_terms(symbol, security, search_terms)
        all_terms.append(new_terms)
        
    write_csv(tickers['symbol'], all_terms, sys.argv[2])



if __name__ == "__main__":
    main()
    
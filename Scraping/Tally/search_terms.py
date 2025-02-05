import pandas as pd
import re
import sys


# read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    return tickers_df


def generate_terms(symbol, security, search_terms):
    searches = []
    
    # add ticker variations and their possessives
    searches.append(symbol)
    searches.append(f'{symbol}s')
    searches.append(f'{symbol}\'s')
    
    searches.append(f"${symbol}")
    searches.append(f"${symbol[0]}{symbol.lower()[1:]}")
    searches.append(f"${symbol}s")
    searches.append(f"${symbol}'s")

    searches.append(security) 
    if security[-1] != 's':
        searches.append(f"{security}s")
        searches.append(f"{security}'s")   
    
    if isinstance(search_terms, str):
        split_terms = search_terms.split('/')
        for term in split_terms:
            if term.strip():
                searches.append(term.strip())
                if term.strip()[-1] != 's':
                    searches.append(f"{term.strip()}s")
                    searches.append(f"{term.strip()}'s")
    list(set(searches))
    return searches
    
def write_csv(symbol, terms, path):
    df = pd.DataFrame({'symbol': symbol, 'search_terms': terms})
    df.to_csv(path, index=False)
    return
    
    
def main():
    # read in csv
    tickers = read_tickers('/uolstore/home/users/sc21hb/Documents/group-project/Scraping/stock_list.csv')

    all_terms = []
    # loop through each row in the dataframe
    for _, row in tickers.iterrows():
        symbol, security, search_terms = row['symbol'], row['security'], row['search_terms']
        
        # generate search terms
        new_terms = generate_terms(symbol, security, search_terms)
        all_terms.append(new_terms)
        
    # write to csv
    write_csv(tickers['symbol'], all_terms, '/uolstore/home/users/sc21hb/Documents/group-project/Scraping/Tally/search_terms.csv')

if __name__ == "__main__":
    main()
    
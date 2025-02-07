import pandas as pd
import re
import sys


# this needs to be finished
ignore = ["A", "ARE", "ALL", "AMP", "AFL", 
          "BALL", "BRO", "BA", 
          "CAT", "C", "COP", "COST",
          "D", "DAY", "DECK", "DD", 
          "ED", 
          "T", "TECH"]


# read in stock tickers from csv file
def read_tickers(path):
    tickers_df = pd.read_csv(path)
    return tickers_df


def generate_terms(symbol, security, search_terms):
    searches = []
    
    if symbol:
        # add ticker variations and their possessives
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
    
def write_csv(symbol, terms, path):
    df = pd.DataFrame({'symbol': symbol, 'search_terms': ['|'.join(term) for term in terms]})
    df.to_csv(path, index=False)
    return
    

def main():   
    # read in csv
    tickers = read_tickers('/uolstore/home/users/sc21hb/Documents/group-project/Scraping/stock_list.csv')

    all_terms = []
    # loop through each row in the dataframe
    for _, row in tickers.iterrows():
        symbol = str(row['symbol']).strip() if pd.notna(row['symbol']) else ""
        security = str(row['security']).strip() if pd.notna(row['security']) else ""
        search_terms = str(row['search_terms']).strip() if pd.notna(row['search_terms']) else ""

        # generate search terms
        new_terms = generate_terms(symbol, security, search_terms)
        all_terms.append(new_terms)
        
    # write to csv
    write_csv(tickers['symbol'], all_terms, '/uolstore/home/users/sc21hb/Documents/group-project/Scraping/Tally/search_terms.csv')

if __name__ == "__main__":
    main()
    
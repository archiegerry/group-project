Closing prices for individual stocks are downloaded from nasdaq - contained in daily_prices.parquet.

Closing prices for S&P 500 index are downloaded from investing.com (nasdaq had too little history). 

The constituent stocks are based on the S&P as of 2024-12-24. We may want to construct our own index of *these* stocks to avoid survivorship bias.


## Stock list

The selected stocks are constituents of the S&P500 as of 2024-12-24, as well as Gamestop. (https://github.com/datasets/s-and-p-500-companies/blob/main/data/constituents.csv)


Each stock is given a 'search_terms' field which is a forward slash '/' separated list of names commonly associated with the stock. 
These were added semi-automatically, mainly by removing common company stop words (Inc., (The), etc).

> df['search_terms'] = df.security.str.replace("(The)", "").str.replace("Inc.", "").str.replace("(Class A)", "").str.replace("(Class B)", "").str.replace("(Class C)", "").str.replace("Company", "").str.replace("Corporation", "").str.replace("Group", "").str.replace(",", "").str.replace(" of ", " ").str.replace("plc", "").str.replace(" & ", " ").str.replace("'", "").str.replace("  ", " ").str.replace("Companies", "").str.replace(".", "")

Some were manual changes, for example, 'Walt Disney Corporation' would simply be referred to as 'Disney'.

Some companies have no search term, if their name is too generic to search or likely to come up with different companies. For these, we can still search for exact matching stock tickers.
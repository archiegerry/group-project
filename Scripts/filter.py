
# CHECKING FOR STOCK/SLANG OVERLAPS IN REDDIT DATA


import csv

# Open CSV files
with open('/uolstore/home/users/sc21hb/Documents/group-project/Scraping/Tally/search_terms.csv', 'r') as csv_stocks, \
     open('/uolstore/home/users/sc21hb/Downloads/wallstreetbets.csv', 'r') as csv_reddit:
    
    # Read Reddit CSV once into memory
    reddit_rows = csv_reddit.read()  # Convert to lowercase for case-insensitive matching
    
    # Read Stock CSV
    stock_reader = csv.reader(csv_stocks)
    next(stock_reader)  # Skip header if present
    
    matched_symbols = []
    
    for row in stock_reader:
        symbol = f" {row[0]} "# First column is the symbol
        
        # Count occurrences of the symbol in all Reddit data
        count = reddit_rows.count(symbol)  # Convert to lowercase to match case-insensitively
        
        if count >= 5:
            matched_symbols.append(symbol)

print(matched_symbols)
print(len(matched_symbols))
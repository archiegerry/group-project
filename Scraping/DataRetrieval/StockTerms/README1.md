# TALLYING REFERENCES

## search_terms.py

Generates search terms from the stock_list csv.  
  
Eg...  

symbol,security,search_terms  
ADBE,Adobe Inc.,Adobe

->   

symbol,search_terms  
ADBE,"['Adobe Inc.s', 'ADOBES', '$ADBEs', 'Adobes', 'adobe inc.s', 'Adobe Inc.', 'adobe inc.', 'adobe', 'adobes', 'ADOBE INC.S', 'Adobe', 'ADOBE', 'ADBE', 'ADBEs', 'ADOBE INC.', '$ADBE']"



## tally.py

Usage:  
> python tally.py <stock_file> <news/reddit> <input_file> <output_file>

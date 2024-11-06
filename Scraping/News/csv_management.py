import os
import glob
import pandas as pd

def combine(folder, root):
    pattern = os.path.join(folder, f"{root}*.csv")
    csv_files = glob.glob(pattern)
    
    # Initialize a list to hold dataframes
    dataframes = []
    
    # Read each CSV file and append the dataframe to the list
    for file in csv_files:
        df = pd.read_csv(file)
        dataframes.append(df)
    
    output_file = os.path.join(folder, f"{root}.csv")
    # Concatenate all dataframes in the list into a single dataframe
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV saved to {output_file}")

    # delete og files
    for file in csv_files:
        if file != output_file:
            os.remove(file)
            print(f"Deleted {file}")

combine('sample_news/theverge/', 'Apple')
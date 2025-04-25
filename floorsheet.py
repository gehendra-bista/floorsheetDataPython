import pandas as pd
import os

# Define the list of file paths
file_paths = [
    'jsonChukulFloorsheetData/2022.txt',
    'jsonChukulFloorsheetData/2023.txt',
    'jsonChukulFloorsheetData/2024.txt',
    'jsonChukulFloorsheetData/2025.txt'
]

# Initialize an empty list to store DataFrames
dfs = []

# Function to read each file into a DataFrame with error handling
def read_file(file_path):
    try:
        # Read tab-separated text file
        df = pd.read_csv(file_path, delimiter='\t')
        # Check for required columns
        required_columns = {'Date', 'symbol', 'buyer', 'seller', 'quantity', 'amount'}
        if not required_columns.issubset(df.columns):
            raise ValueError(f"Missing required columns in {file_path}")
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

# Read each file and append valid DataFrames to the list
for file_path in file_paths:
    df = read_file(file_path)
    if df is not None:
        dfs.append(df)

# Check if any DataFrames were loaded
if not dfs:
    print("No valid DataFrames were created from the provided file paths.")
else:
    # Concatenate all DataFrames into one
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Perform buyer analysis
    try:
        buyer_analysis = combined_df.groupby(['Date', 'symbol', 'buyer']).agg({
            'quantity': 'sum',
            'amount': 'sum'
        })
        # Create buyer_key by joining group keys with ';'
        buyer_analysis.index = buyer_analysis.index.map(lambda x: ';'.join(map(str, x)))
        buyer_analysis = buyer_analysis.reset_index()
        buyer_analysis.columns = ['buyer_key', 'quantity_buyer', 'amount_buyer']
    except KeyError as e:
        print(f"Error in buyer analysis: Missing column {e}")
        buyer_analysis = pd.DataFrame()
    
    # Perform seller analysis
    try:
        seller_analysis = combined_df.groupby(['Date', 'symbol', 'seller']).agg({
            'quantity': 'sum',
            'amount': 'sum'
        })
        # Create seller_key by joining group keys with ';'
        seller_analysis.index = seller_analysis.index.map(lambda x: ';'.join(map(str, x)))
        seller_analysis = seller_analysis.reset_index()
        seller_analysis.columns = ['seller_key', 'quantity_seller', 'amount_seller']
    except KeyError as e:
        print(f"Error in seller analysis: Missing column {e}")
        seller_analysis = pd.DataFrame()
    
    # Merge buyer and seller analyses if both are valid
    if not buyer_analysis.empty and not seller_analysis.empty:
        merged_df = pd.merge(
            buyer_analysis,
            seller_analysis,
            left_on='buyer_key',
            right_on='seller_key',
            how='outer'
        )
        
        # Create a unified key column
        merged_df['key'] = merged_df['buyer_key'].combine_first(merged_df['seller_key'])
        
        # Split the 'key' column into 'Date', 'Script', and 'Broker'
        split_keys = merged_df['key'].str.split(';', expand=True)
        if split_keys.shape[1] >= 3:  # Ensure there are at least 3 parts
            merged_df['Date'] = split_keys[0]
            merged_df['Script'] = split_keys[1]
            merged_df['Broker'] = split_keys[2]
        else:
            print("Warning: 'key' column does not contain enough parts to split into Date, Script, and Broker")
        
        # Reorder columns to place split columns first
        columns = ['Date', 'Script', 'Broker'] + [col for col in merged_df.columns if col not in ['Date', 'Script', 'Broker', 'key', 'buyer_key', 'seller_key']]
        export_df = merged_df[columns]
        
        # Export to CSV with tab separation
        output_file = 'buyerSellerData.csv'
        export_df.to_csv(output_file, sep='\t', index=False)
        
        # Print confirmation
        print(f"Merged analysis exported to {output_file} with key split into Date, Script, and Broker, excluding buyer_key and seller_key")
        print("All Done!!!!!!!!!!!!!!!")
    else:
        print("Unable to perform merge due to errors in analysis.")
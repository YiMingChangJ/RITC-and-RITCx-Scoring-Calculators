import os
import pandas as pd

# --- Configuration ---
# Set this to the name of your main results folder
root_folder = r"C:\Users\yiming.chang\source\repos\NewScoringPortal\Previous_Competition_Results\Sample result data\1 BP Commodities"
output_file = "H1SH1.csv"
# ---------------------

all_data = []

print(f"Starting to process files in: {root_folder}")

# Walk through the directory structure
for dirpath, dirnames, filenames in os.walk(root_folder):
    for filename in filenames:
        if filename.endswith(".csv"):
            # Construct the full file path
            file_path = os.path.join(dirpath, filename)
            
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Check if required columns exist
                if 'TraderID' in df.columns and 'NLV' in df.columns:
                    all_data.append(df)
                    print(f"  Successfully read: {file_path}")
                else:
                    print(f"  Skipped (missing columns): {file_path}")
                    
            except Exception as e:
                print(f"  Error reading {file_path}: {e}")

if not all_data:
    print("No CSV files with 'TraderID' and 'NLV' columns were found.")
else:
    # Combine all the individual DataFrames into one
    master_df = pd.concat(all_data, ignore_index=True)
    
    print("\nAll files combined. Aggregating data...")
    
    # --- Perform the Aggregation ---
    
    # 1. Create the TeamID column
    # Ensure TraderID is a string before splitting
    master_df['TraderID'] = master_df['TraderID'].astype(str)
    master_df['TeamID'] = master_df['TraderID'].str.split('-').str[0]
    
    # 2. Group by the new TeamID column and sum the NLV
    team_nlv = master_df.groupby('TeamID')['NLV'].sum().reset_index()
    
    # 3. Rename NLV for clarity
    team_nlv = team_nlv.rename(columns={'NLV': 'Total_NLV'})
    
    # 4. Save the final aggregated data to a new CSV
    team_nlv.to_csv(output_file, index=False)
    
    print(f"\nDone! Aggregated data saved to: {output_file}")
    print("Preview of the final table:")
    print(team_nlv.head())
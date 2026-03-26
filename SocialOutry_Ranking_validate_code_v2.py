import pandas as pd
import numpy as np

def process_trading_rankings(file_path, final_price, contract_multiplier, commission):
    # 1. Load the data 
    # (Change 'read_excel' to 'read_csv' if your file is actually a .csv)
    df = pd.read_csv(file_path)

    # Clean team names to be uppercase to avoid mismatching (e.g., 'uiuc' vs 'UIUC')
    df['BuyTeam'] = df['BuyTeam'].astype(str).str.upper()
    df['SellTeam'] = df['SellTeam'].astype(str).str.upper()

    # 2. Split data into Buyer and Seller perspectives
    # For Buyers (Long)
    buyers = df[['BuyTeam', 'SellTeam', 'Size', 'Price']].copy()
    buyers.rename(columns={'BuyTeam': 'Team', 'SellTeam': 'Counterparty'}, inplace=True)
    buyers['Side'] = 'BUY'

    # For Sellers (Short)
    sellers = df[['SellTeam', 'BuyTeam', 'Size', 'Price']].copy()
    sellers.rename(columns={'SellTeam': 'Team', 'BuyTeam': 'Counterparty'}, inplace=True)
    sellers['Side'] = 'SELL'

    # Combine them into a single ledger of trades per team
    trades = pd.concat([buyers, sellers], ignore_index=True)

    # 3. Calculate Unique Counterparties
    cp_df = trades.groupby('Team')['Counterparty'].nunique().reset_index()
    cp_df.rename(columns={'Counterparty': 'Unique_Counterparties'}, inplace=True)

    # 4. Calculate PnL
    # Apply your exact formulas based on the trade side
    trades['Trade_PnL'] = np.where(
        trades['Side'] == 'BUY',
        (final_price - trades['Price']) * trades['Size'] * contract_multiplier - trades['Size'] * commission,
        (trades['Price'] - final_price) * trades['Size'] * contract_multiplier - trades['Size'] * commission
    )

    # Aggregate total PnL per team
    pnl_df = trades.groupby('Team')['Trade_PnL'].sum().reset_index()
    pnl_df.rename(columns={'Trade_PnL': 'Total_PnL'}, inplace=True)

    # 5. Merge metrics and Rank
    results = pd.merge(cp_df, pnl_df, on='Team')

    # Rank Counterparties: Larger number = Better Rank (1)
    results['Rank_CP'] = results['Unique_Counterparties'].rank(ascending=False, method='min')

    # Rank PnL: Larger PnL = Better Rank (1)
    results['Rank_PnL'] = results['Total_PnL'].rank(ascending=False, method='min')

    # Calculate Average Rank
    results['Average_Rank'] = (results['Rank_CP'] + results['Rank_PnL']) / 2.0

    # Final Rank: Smaller average number = Better Final Rank (1)
    results['Final_Rank'] = results['Average_Rank'].rank(ascending=True, method='min')

    # Sort the final output by the Final Rank for readability
    results = results.sort_values('Final_Rank')

    # 6. Save to CSV
    output_filename = 'final_team_rankings.csv'
    results.to_csv(output_filename, index=False)
    print(f"Calculations complete! Results saved to {output_filename}")
    
    return results

# ==========================================
# Run the Code Here
# ==========================================
# Set your parameters
FILE_PATH = r'C:\Users\yiming.chang\Downloads\Counterparty_Summary.csv' # Replace with your actual file name
FINAL_PRICE = 998.0              # Replace with your constant Final Price
CONTRACT_MULTIPLIER = 10          # Replace with your constant multiplier
COMMISSION = 1.0                  # Replace with your constant commission

# Execute
final_rankings = process_trading_rankings(
    file_path=FILE_PATH, 
    final_price=FINAL_PRICE, 
    contract_multiplier=CONTRACT_MULTIPLIER, 
    commission=COMMISSION
)

# Display the top 5 results in the console
print(final_rankings.head())

import pandas as pd

df = pd.read_csv(r'C:\Users\yiming.chang\Downloads\Counterparty_Summary.csv') 

buys = df[['BuyTeam', 'SellTeam']].rename(columns={'BuyTeam': 'Team', 'SellTeam': 'Counterparty'})

sells = df[['SellTeam', 'BuyTeam']].rename(columns={'SellTeam': 'Team', 'BuyTeam': 'Counterparty'})

all_trades = pd.concat([buys, sells])
all_trades['Team'] = all_trades['Team'].str.upper().str.strip()
all_trades['Counterparty'] = all_trades['Counterparty'].str.upper().str.strip()

unique_counts = all_trades.groupby('Team')['Counterparty'].nunique().reset_index()
unique_counts.columns = ['Team', 'Unique_Counterparties']

unique_counts['Rank'] = unique_counts['Unique_Counterparties'].rank(ascending=False, method='min').astype(int)

unique_counts = unique_counts.sort_values(by='Rank')
unique_counts.to_csv('team_ranks.csv', index=False)

print("Ranking complete. Results saved to 'team_ranks.csv'.")
print(unique_counts.head())
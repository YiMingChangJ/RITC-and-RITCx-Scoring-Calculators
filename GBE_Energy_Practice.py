#!/usr/bin/env python3
"""
RIT CaseRankAnalyzer (Single File Version)
------------------------------------------
Reads ONE Excel results file, parses TraderIDs (Team-Role),
and generates Top 10 Leaderboards for:
 1. Traders (T1/T2)
 2. Distributors (D)
 3. Producers (P)
 4. Teams Overall (Sum of all roles)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

class SingleFileRankAnalyzer:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
        self.leaderboards = {}

    def _clean_currency(self, series: pd.Series) -> pd.Series:
        """Cleans currency strings (e.g., '$1,234.56', '(500)') into floats."""
        s = series.astype(str).str.replace(r"\s+", "", regex=True)
        s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
        s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    def load_data(self):
        """Reads the single Excel file and prepares the data."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        # Read Excel
        try:
            raw_df = pd.read_excel(self.file_path)
        except Exception as e:
            raise ValueError(f"Could not read Excel file. Error: {e}")

        # --- Column Mapping ---
        # Normalize column names to lowercase to find 'traderid' and 'nlv'/'pnl'
        raw_df.columns = [c.strip() for c in raw_df.columns]
        low_cols = {c.lower(): c for c in raw_df.columns}

        # 1. Find TraderID
        if "traderid" in low_cols:
            tid_col = low_cols["traderid"]
        else:
            raise KeyError("Could not find a 'TraderID' column in the Excel file.")

        # 2. Find NLV (or P&L)
        if "nlv" in low_cols:
            nlv_col = low_cols["nlv"]
        elif "total nlv" in low_cols:
            nlv_col = low_cols["total nlv"]
        else:
            # Fallback to PnL search
            pnl_cands = [c for c in low_cols if "pnl" in c or "p&l" in c]
            if pnl_cands:
                nlv_col = pnl_cands[0]
            else:
                raise KeyError("Could not find 'NLV' or 'P&L' column.")

        # Extract and Clean Data
        self.df = pd.DataFrame()
        self.df["TraderID"] = raw_df[tid_col].astype(str)
        self.df["NLV"] = self._clean_currency(raw_df[nlv_col])

        # Parse "TEAM-ROLE" (e.g. FQAR-T1 -> Root: FQAR, Suffix: T1)
        # We split by the *last* hyphen to handle names like "Team-A-T1" correctly
        split_data = self.df["TraderID"].str.rsplit("-", n=1)
        
        self.df["Root"] = split_data.apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
        self.df["Suffix"] = split_data.apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else "Unknown")

        print(f"Loaded {len(self.df)} rows from {os.path.basename(self.file_path)}")

    def generate_leaderboards(self):
        """Filters data by role suffix and aggregates for teams."""
        if self.df is None or self.df.empty:
            return

        def get_top_10(data, name_col="TraderID"):
            return (data.sort_values(by="NLV", ascending=False)
                    .head(10)
                    .reset_index(drop=True)[[name_col, "NLV"]])

        # 1. Top 10 Traders (Suffix T1 or T2)
        # Note: You can separate them if needed, this combines them like your screenshot example
        mask_traders = self.df["Suffix"].str.upper().isin(["T1", "T2"])
        self.leaderboards["Top 10 Traders"] = get_top_10(self.df[mask_traders])

        # 2. Top 10 Distributors (Suffix D)
        mask_dist = self.df["Suffix"].str.upper() == "D"
        self.leaderboards["Top 10 Distributors"] = get_top_10(self.df[mask_dist])

        # 3. Top 10 Producers (Suffix P)
        mask_prod = self.df["Suffix"].str.upper() == "P"
        self.leaderboards["Top 10 Producers"] = get_top_10(self.df[mask_prod])

        # 4. Top 10 Teams Overall (Sum of all roles sharing the same Root)
        team_agg = self.df.groupby("Root", as_index=False)["NLV"].sum()
        self.leaderboards["Top 10 Teams Overall"] = get_top_10(team_agg, name_col="Root")

    def save_results(self, output_name="Leaderboard_Results.xlsx"):
        """Saves the leaderboards to a styled Excel file."""
        if not self.leaderboards:
            print("No leaderboards generated.")
            return

        # Handle path/filename
        directory = os.path.dirname(self.file_path)
        save_path = os.path.join(directory, output_name)

        try:
            writer = pd.ExcelWriter(save_path, engine="xlsxwriter")
            workbook = writer.book
            sheet = workbook.add_worksheet("Rankings")

            # --- Formats ---
            fmt_header_orange = workbook.add_format({'bold': True, 'bg_color': '#FCE4D6', 'border': 1}) # Traders
            fmt_header_red = workbook.add_format({'bold': True, 'bg_color': '#F4CCCC', 'border': 1})    # Distributors
            fmt_header_green = workbook.add_format({'bold': True, 'bg_color': '#D9EAD3', 'border': 1})  # Producers
            fmt_header_blue = workbook.add_format({'bold': True, 'bg_color': '#C9DAF8', 'border': 1})   # Teams
            
            fmt_currency = workbook.add_format({'num_format': '$#,##0'})
            fmt_bold = workbook.add_format({'bold': True})

            # Define Layout order
            layout = [
                ("Top 10 Traders", fmt_header_orange),
                ("Top 10 Distributors", fmt_header_red),
                ("Top 10 Producers", fmt_header_green),
                ("Top 10 Teams Overall", fmt_header_blue)
            ]

            # Write tables side-by-side
            row_start = 1
            col_start = 1

            for title, header_fmt in layout:
                if title not in self.leaderboards: continue
                
                df = self.leaderboards[title]
                if df.empty: 
                    # Skip empty tables but move column cursor
                    col_start += 3
                    continue

                # 1. Title Row
                sheet.write(row_start, col_start, title, header_fmt)
                sheet.write(row_start, col_start + 1, "", header_fmt) # Merge look-alike

                # 2. Column Headers
                sheet.write(row_start + 1, col_start, "TraderID", fmt_bold)
                sheet.write(row_start + 1, col_start + 1, "NLV", fmt_bold)

                # 3. Data Rows
                for i, row in df.iterrows():
                    name = row.iloc[0] # TraderID or Team Root
                    val = row["NLV"]
                    
                    sheet.write(row_start + 2 + i, col_start, name)
                    sheet.write(row_start + 2 + i, col_start + 1, val, fmt_currency)

                # Move cursor right for the next table (3 columns spacing)
                col_start += 3
                
                # Simple wrap logic if needed (optional)
                if col_start > 12:
                    col_start = 1
                    row_start += 15

            writer.close()
            print(f"Successfully saved rankings to: {save_path}")

        except Exception as e:
            print(f"Error saving file: {e}")

# ========================== Main Execution ==========================
if __name__ == "__main__":
    # 1. DEFINE YOUR FILE PATH HERE
    # Use 'r' before string for Windows paths
    input_file = r"C:\Users\yiming.chang\OneDrive - University of Toronto\Desktop\Yi-Ming Chang\Educational Developer\RITC\Results.xlsx"

    analyzer = SingleFileRankAnalyzer(input_file)
    
    try:
        analyzer.load_data()
        analyzer.generate_leaderboards()
        analyzer.save_results("Rankings_Output.xlsx")
    except Exception as ex:
        print(f"An error occurred: {ex}")


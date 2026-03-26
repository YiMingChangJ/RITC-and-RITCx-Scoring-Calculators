#!/usr/bin/env python3
"""
Rotman BMO Finance Research and Trading Lab, University of Toronto (C)
All rights reserved.

CaseRankAnalyzer — Individual (Trader) Scoring with Heat-Accurate Mapping
and "Missed Rounds" Handling (Complete Script)

What this does
--------------
• Reads Results.xlsx (or any .xlsx) from subfolders under main_path.
• Extracts CASE and numeric HEAT from folder names to prevent mis-mapped columns.
• Builds per-TRADER tables (NLV per heat, rank per heat).
• If a trader misses early heats, densifies the grid so those heats appear with NLV=0
  and rank last (via NLV_for_rank = -inf after aggregation).
• Computes average ranks per case, case ranks, and overall rank.
• Robust save: handles Windows/OneDrive file locks and long paths.

Output columns (examples)
-------------------------
TraderID, FirstName, LastName, root,
NLV_LT3_Heat 1, …, Rank_LT3_Heat 1, …,
avg_rank_LT3, case_rank_LT3, average_case_rank, overall_rank
"""

import os
import re
import glob
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional, List

import numpy as np
import pandas as pd


# ----------------------- Path utilities -----------------------
def _shorten_path(p: str, max_len: int = 230) -> str:
    """If Windows path is too long, write to %TEMP% with same filename."""
    p_obj = Path(p)
    if len(str(p_obj)) <= max_len:
        return str(p_obj)
    return str(Path(os.getenv("TEMP", ".")) / p_obj.name)


def _timestamped(name: str, suffix: str = ".xlsx") -> str:
    """Insert a timestamp before the suffix (e.g., results_20251108-091501.xlsx)."""
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    stem = Path(name).stem
    return f"{stem}_{ts}{suffix}"


# ======================= Analyzer =======================
class CaseRankAnalyzer:
    def __init__(self, main_path: str) -> None:
        self.main_path = main_path
        self.all_data: Optional[pd.DataFrame] = None
        self.wide: Optional[pd.DataFrame] = None

    # ---------- helpers ----------
    @staticmethod
    def _extract_case(folder_name: str) -> str:
        """
        Map folder names to case codes (e.g. 'LT3', 'Algo2').
        NOTE: fixed lowercasing bug for 'algo2'.
        """
        low = folder_name.lower()
        if "lt3" in low:
            return "LT3"
        if "algo2" in low:
            return "Algo2"
        return "Unknown"

    @staticmethod
    def _extract_heat_num(folder_name: str) -> Optional[int]:
        """
        Prefer explicit 'heat N'; otherwise use the LAST integer found in the folder name.
        Examples:
          'LT3_Heat 8' -> 8
          'LT3-H9'     -> 9
          'LT3_11'     -> 11
          'Heat_02'    -> 2
        """
        m = re.search(r"heat[\s_\-]*?(\d+)", folder_name, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
        ints = re.findall(r"(\d+)", folder_name)
        if ints:
            return int(ints[-1])
        return None

    @staticmethod
    def _coerce_money_like(s: pd.Series) -> pd.Series:
        """Coerce money-like strings ($, commas, spaces, and (negatives)) to float."""
        s = s.astype(str).str.replace(r"\s+", "", regex=True)
        s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
        s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    @staticmethod
    def _pick_nlv_series(df: pd.DataFrame) -> pd.Series:
        """
        Select the NLV column robustly; fall back to PnL if necessary.
        More tolerant of headers like 'Total NLV', 'P&L', 'Total PnL', etc.
        """
        low = {str(c).strip().lower(): c for c in df.columns}

        # Direct match 'nlv'
        if "nlv" in low:
            s = df[low["nlv"]]
        else:
            # Any column containing 'nlv'
            nlv_cands = [k for k in low if "nlv" in k]
            if nlv_cands:
                picked = sorted(nlv_cands, key=len)[0]  # prefer shortest
                s = df[low[picked]]
            else:
                # Fallback to PnL / P&L / similar
                pnl_cands = [k for k in low if "pnl" in k or "p&l" in k]
                if not pnl_cands:
                    raise KeyError("Could not locate NLV / PnL column in results sheet.")
                picked = sorted(pnl_cands, key=len)[0]
                s = df[low[picked]]

        return CaseRankAnalyzer._coerce_money_like(s)

    @staticmethod
    def _pick_str_col(df: pd.DataFrame, name: str, default: str = "Unknown") -> pd.Series:
        """Safely pick a string column; use default if missing."""
        low = {str(c).strip().lower(): c for c in df.columns}
        if name.lower() in low:
            return df[low[name.lower()]].astype(str).fillna(default)
        return pd.Series([default] * len(df), index=df.index, dtype="string")

    @staticmethod
    def _pick_traderid(df: pd.DataFrame) -> pd.Series:
        low = {str(c).strip().lower(): c for c in df.columns}
        if "traderid" in low:
            return df[low["traderid"]].astype(str)
        raise KeyError("Column 'TraderID' not found in results sheet.")

    # ---------- load with strict heat mapping ----------
    def load_and_prepare(self) -> None:
        """
        1) Collect files:
             - Prefer .../<HeatFolder>/Results.xlsx
             - If none, fall back to .../<HeatFolder>/*.xlsx (first sheet)
        2) Extract case and numeric heat from each folder
        3) Deduplicate (case, heat_num) by keeping the newest file
        4) Build rows with exact heat_num and label 'Heat {heat_num}'
        """
        files = glob.glob(os.path.join(self.main_path, "*", "Results.xlsx"))
        if not files:
            files = glob.glob(os.path.join(self.main_path, "*", "*.xlsx"))
        if not files:
            raise FileNotFoundError(f"No .xlsx files found under {self.main_path!r}.")

        # Choose newest per (case, heat_num) to avoid collisions
        chosen: Dict[Tuple[str, int], str] = {}
        for fp in files:
            folder = os.path.basename(os.path.dirname(fp))
            case = self._extract_case(folder)
            heat_num = self._extract_heat_num(folder)
            if heat_num is None:
                # Skip folders we cannot map to a heat number to avoid misplacement
                continue
            key = (case, heat_num)
            if key not in chosen or os.path.getmtime(fp) > os.path.getmtime(chosen[key]):
                chosen[key] = fp

        if not chosen:
            raise ValueError("No usable heat numbers could be extracted from folder names.")

        rows: List[pd.DataFrame] = []
        for (case, heat_num), fp in sorted(chosen.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            df = pd.read_excel(fp, sheet_name=0)

            trader_id = self._pick_traderid(df)
            first_name = self._pick_str_col(df, "FirstName", default="Unknown")
            last_name = self._pick_str_col(df, "LastName", default="Unknown")
            nlv_series = self._pick_nlv_series(df)

            # root is typically the team prefix (before '-'); we keep it as a label only
            root = trader_id.astype(str).str.split("-").str[0]

            rows.append(pd.DataFrame({
                "TraderID": trader_id,
                "FirstName": first_name,
                "LastName": last_name,
                "root": root,
                "case": case,
                "heat_num": int(heat_num),               # numeric key for ordering
                "heat": f"Heat {int(heat_num)}",         # pretty label
                "NLV": nlv_series,
            }))

        self.all_data = pd.concat(rows, ignore_index=True)
        if self.all_data.empty:
            raise ValueError("Loaded data is empty after concatenation.")

    # ---------- build individual table with correct heat alignment & missed-rounds handling ----------
    def build_table(self) -> None:
        if self.all_data is None or self.all_data.empty:
            raise ValueError("No data loaded. Call load_and_prepare() first.")

        df = self.all_data.copy()
        df["NLV"] = pd.to_numeric(df["NLV"], errors="coerce").fillna(0.0)

        # Aggregate to TRADER per (case, heat_num) — in case multiple rows exist.
        trader_heat = (
            df.groupby(["TraderID", "FirstName", "LastName", "root", "case", "heat_num"], as_index=False)
              .agg(NLV=("NLV", "sum"))
        )

        # ---- DENSIFY GRID FOR MISSED ROUNDS ----
        # All heats per case present in data:
        case_heat = trader_heat[["case", "heat_num"]].drop_duplicates()

        # All (TraderID, FirstName, LastName, root, case) where trader appears at least once:
        trader_case = trader_heat[["TraderID", "FirstName", "LastName", "root", "case"]].drop_duplicates()

        # Cartesian join → ensure every trader-case has every heat of that case:
        full_index = trader_case.merge(case_heat, on="case", how="left")

        # Left-join existing results; missing heats → NLV = 0
        trader_heat_full = full_index.merge(
            trader_heat,
            on=["TraderID", "FirstName", "LastName", "root", "case", "heat_num"],
            how="left"
        )
        trader_heat_full["NLV"] = trader_heat_full["NLV"].fillna(0.0)
        trader_heat_full["heat"] = "Heat " + trader_heat_full["heat_num"].astype(int).astype(str)

        # Ranking rule: AFTER aggregation → -inf if trader NLV == 0 for that heat
        # This makes non-participation (or exact zero P&L) rank last in that heat.
        trader_heat_full["NLV_for_rank"] = np.where(
            trader_heat_full["NLV"] == 0.0,
            -np.inf,
            trader_heat_full["NLV"]
        )

        # Min rank within each (case, heat_num): higher NLV is better (1 is best)
        trader_heat_full["heat_rank"] = (
            trader_heat_full.groupby(["case", "heat_num"], group_keys=False)["NLV_for_rank"]
                            .rank(method="min", ascending=False)
        )

        # ----- Pivot with strict numeric ordering -----
        # NLV wide
        nlv_wide = trader_heat_full.pivot_table(
            index=["TraderID", "FirstName", "LastName", "root"],
            columns=["case", "heat_num"],
            values="NLV",
            aggfunc="first"
        ).sort_index(axis=1, level=[0, 1])

        # Rank wide
        rank_wide = trader_heat_full.pivot_table(
            index=["TraderID", "FirstName", "LastName", "root"],
            columns=["case", "heat_num"],
            values="heat_rank",
            aggfunc="first"
        ).sort_index(axis=1, level=[0, 1])

        # Flatten to names like NLV_LT3_Heat 1, Rank_LT3_Heat 1
        nlv_wide.columns = [f"NLV_{case}_Heat {int(h)}" for (case, h) in nlv_wide.columns.to_list()]
        rank_wide.columns = [f"Rank_{case}_Heat {int(h)}" for (case, h) in rank_wide.columns.to_list()]

        wide = pd.concat([nlv_wide, rank_wide], axis=1).reset_index()

        # ---- Average heat ranks per case ----
        avg_ranks = (
            trader_heat_full.groupby(
                ["TraderID", "FirstName", "LastName", "root", "case"], as_index=False
            )["heat_rank"]
            .mean()
            .rename(columns={"heat_rank": "avg_rank"})
        )

        avg_ranks_wide = avg_ranks.pivot_table(
            index=["TraderID", "FirstName", "LastName", "root"],
            columns="case",
            values="avg_rank"
        ).reset_index()
        avg_ranks_wide = avg_ranks_wide.rename(
            columns={
                c: f"avg_rank_{c}"
                for c in avg_ranks_wide.columns
                if c not in ["TraderID", "FirstName", "LastName", "root"]
            }
        )

        wide = wide.merge(avg_ranks_wide, on=["TraderID", "FirstName", "LastName", "root"], how="left")

        # ---- Case rank (lower is better) ----
        for col in [c for c in wide.columns if c.startswith("avg_rank_")]:
            case_name = col.replace("avg_rank_", "")
            wide[f"case_rank_{case_name}"] = wide[col].rank(method="min", ascending=True)

        # ---- Overall rank ----
        case_cols = [c for c in wide.columns if c.startswith("case_rank_")]
        if case_cols:
            wide["average_case_rank"] = wide[case_cols].mean(axis=1, skipna=True)
            wide["overall_rank"] = wide["average_case_rank"].rank(method="min", ascending=True)
        else:
            wide["average_case_rank"] = np.nan
            wide["overall_rank"] = np.nan

        # If you truly don't want any team-ish identifier at all,
        # you can drop 'root' here:
        # wide = wide.drop(columns=["root"])

        self.wide = wide

    # ---------- robust save ----------
    def save(self, filename: str) -> None:
        """
        Save the wide table to Excel.
        - Handles Windows file locks (saves timestamped copy if locked)
        - Guards against too-long paths (writes to TEMP)
        """
        if self.wide is None or self.wide.empty:
            print("No table to save. Run build_table() first.")
            return

        out_file = filename if os.path.isabs(filename) else os.path.join(self.main_path, filename)
        out_file = _shorten_path(out_file)

        # Ensure directory exists
        try:
            d = os.path.dirname(out_file)
            if d:
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass

        try:
            self.wide.to_excel(out_file, index=False, engine="xlsxwriter")
            print(f"Saved {out_file}")
            return
        except ImportError:
            try:
                self.wide.to_excel(out_file, index=False)
                print(f"Saved {out_file}")
                return
            except PermissionError:
                pass
        except PermissionError:
            pass

        # Timestamped sibling if locked
        try:
            alt_file = os.path.join(os.path.dirname(out_file) or ".", _timestamped(os.path.basename(out_file)))
            try:
                self.wide.to_excel(alt_file, index=False, engine="xlsxwriter")
                print(f"Target file locked. Saved a new copy instead: {alt_file}")
                return
            except ImportError:
                self.wide.to_excel(alt_file, index=False)
                print(f"Target file locked. Saved a new copy instead: {alt_file}")
                return
        except Exception:
            # Final fallback: TEMP
            temp_file = os.path.join(os.getenv("TEMP", "."), _timestamped(os.path.basename(out_file)))
            try:
                self.wide.to_excel(temp_file, index=False, engine="xlsxwriter")
            except ImportError:
                self.wide.to_excel(temp_file, index=False)
            print(f"Saved to TEMP due to lock/path issue: {temp_file}")


# ========================== Runner ==========================
if __name__ == "__main__":
    main_path = r"C:\Users\yiming.chang\OneDrive - University of Toronto\Desktop\Yi-Ming Chang\Educational Developer\Userlist_generator\Userlist_generator\HS"
    analyzer = CaseRankAnalyzer(main_path)
    analyzer.load_and_prepare()
    analyzer.build_table()
    analyzer.save("RITCxQuestrom2025-Trader_Results.xlsx")

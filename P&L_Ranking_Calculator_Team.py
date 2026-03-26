#!/usr/bin/env python3
"""
Rotman BMO Finance Research and Trading Lab, University of Toronto (C)
All rights reserved.

CaseRankAnalyzer — Heat-Accurate Team Ranking (Complete Script, with "missed rounds" handling)

What’s new in this version
--------------------------
If a team did not participate in early heats of a case (so there was NO row for that team/heat),
we now:
  • Densify the grid to include ALL heats of that case for every team that appears in that case,
  • Fill missing NLV with 0 for those heats,
  • Apply the ranking rule AFTER aggregation: if team NLV == 0 for that heat ⇒ NLV_for_rank = -inf
    (so they rank last in that heat).

This fixes “empty cells” (NaN) for early heats and ensures they’re treated as zero P&L and ranked last.

It also keeps the earlier fix that extracts the numeric heat from folder names to prevent mis-mapped data.
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
        self.team_wide: Optional[pd.DataFrame] = None

    # ---------- helpers ----------
    @staticmethod
    def _extract_case(folder_name: str) -> str:
        low = folder_name.lower()
        if "arb" in low:
            return "Stats Arb"
        if "etf" in low:
            return "ETF Arb"
        return "Unknown"

    @staticmethod
    def _extract_heat_num(folder_name: str) -> Optional[int]:
        """
        Prefer explicit 'heat N'; otherwise use the LAST integer found in the folder name.
        Examples that work:
          'LT3_Heat 8' -> 8
          'LT3-H9' -> 9
          'LT3_11' -> 11
          'Heat_02' -> 2
        """
        m = re.search(r"heat[\s_\-]*?(\d+)", folder_name, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
        ints = re.findall(r"(\d+)", folder_name)
        if ints:
            return int(ints[-1])
        return None

    @staticmethod
    def _pick_team_series(df: pd.DataFrame) -> pd.Series:
        """Derive TeamID from TraderID or TeamID (prefix before '-')."""
        low = {str(c).strip().lower(): c for c in df.columns}
        if "traderid" in low:
            base = df[low["traderid"]].astype(str)
        elif "teamid" in low:
            base = df[low["teamid"]].astype(str)
        else:
            raise KeyError("Neither 'TraderID' nor 'TeamID' column found in results sheet.")
        return base.str.split("-").str[0]

    @staticmethod
    def _coerce_money_like(s: pd.Series) -> pd.Series:
        """Coerce money-like values ($, commas, spaces, and (negatives)) to float."""
        s = s.astype(str).str.replace(r"\s+", "", regex=True)
        s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
        s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    @staticmethod
    def _pick_nlv_series(df: pd.DataFrame) -> pd.Series:
        """Select the NLV column robustly; fall back to PnL if necessary."""
        low = {str(c).strip().lower(): c for c in df.columns}
        if "nlv" in low:
            s = df[low["nlv"]]
        else:
            cands = [k for k in low if re.search(r"\bnlv\b", k)]
            if cands:
                s = df[low[sorted(cands, key=len)[0]]]
            else:
                cands = [k for k in low if re.search(r"\bp&?nl\b", k)]
                if not cands:
                    raise KeyError("Could not locate NLV / PnL column in results sheet.")
                s = df[low[sorted(cands, key=len)[0]]]
        return CaseRankAnalyzer._coerce_money_like(s)

    # ---------- load with strict heat mapping ----------
    def load_and_prepare(self) -> None:
        """
        1) Collect files:
             - Prefer .../<HeatFolder>/Results.xlsx
             - If none, fall back to .../<HeatFolder>/*.xlsx (first sheet)
        2) Extract case and numeric heat from each folder
        3) Build rows with exact heat_num and label 'Heat {heat_num}'
        4) Deduplicate (case, heat_num) by keeping the newest file
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
            team_series = self._pick_team_series(df)
            nlv_series = self._pick_nlv_series(df)

            rows.append(pd.DataFrame({
                "TeamID": team_series,
                "case": case,
                "heat_num": int(heat_num),               # numeric key for ordering
                "heat": f"Heat {int(heat_num)}",         # pretty label
                "NLV": nlv_series,
            }))

        self.all_data = pd.concat(rows, ignore_index=True)
        if self.all_data.empty:
            raise ValueError("Loaded data is empty after concatenation.")

    # ---------- build table with correct heat alignment & missing participation handling ----------
    def build_table(self) -> None:
        if self.all_data is None or self.all_data.empty:
            raise ValueError("No data loaded. Call load_and_prepare() first.")

        df = self.all_data.copy()
        df["NLV"] = pd.to_numeric(df["NLV"], errors="coerce").fillna(0.0)

        # Aggregate to team per (case, heat_num)
        team_heat = (
            df.groupby(["TeamID", "case", "heat_num"], as_index=False)
              .agg(NLV=("NLV", "sum"))
        )

        # ---- DENSIFY GRID FOR MISSED ROUNDS ----
        # For each (case), build the complete set of heats present in data:
        case_heat = team_heat[["case", "heat_num"]].drop_duplicates()

        # For each TeamID, restrict to cases where the team appeared at least once:
        team_case = team_heat[["TeamID", "case"]].drop_duplicates()

        # Cartesian join → for every (TeamID, case) include ALL heats of that case
        full_index = team_case.merge(case_heat, on="case", how="left")

        # Left-join existing results to the full grid, then fill missing heats with NLV=0
        team_heat_full = full_index.merge(team_heat, on=["TeamID", "case", "heat_num"], how="left")
        team_heat_full["NLV"] = team_heat_full["NLV"].fillna(0.0)

        # Pretty label after densifying
        team_heat_full["heat"] = "Heat " + team_heat_full["heat_num"].astype(int).astype(str)

        # Rank rule: AFTER aggregation → -inf if team NLV == 0 for that heat
        team_heat_full["NLV_for_rank"] = np.where(team_heat_full["NLV"] == 0.0,
                                                  -np.inf,
                                                  team_heat_full["NLV"])

        # Min rank within each (case, heat_num): higher NLV is better (1 is best)
        team_heat_full["heat_rank"] = (
            team_heat_full.groupby(["case", "heat_num"], group_keys=False)["NLV_for_rank"]
                          .rank(method="min", ascending=False)
        )

        # ----- Pivot with strict numeric ordering -----
        nlv_wide = team_heat_full.pivot_table(
            index="TeamID", columns=["case", "heat_num"], values="NLV", aggfunc="first"
        ).sort_index(axis=1, level=[0, 1])

        rank_wide = team_heat_full.pivot_table(
            index="TeamID", columns=["case", "heat_num"], values="heat_rank", aggfunc="first"
        ).sort_index(axis=1, level=[0, 1])

        # Flatten with expected naming scheme: NLV_<case>_Heat <num>, Rank_<case>_Heat <num>
        nlv_wide.columns = [f"NLV_{case}_Heat {int(h)}" for (case, h) in nlv_wide.columns.to_list()]
        rank_wide.columns = [f"Rank_{case}_Heat {int(h)}" for (case, h) in rank_wide.columns.to_list()]

        wide = pd.concat([nlv_wide, rank_wide], axis=1).reset_index()

        # Average heat ranks per case (includes zero-filled heats, so missed rounds hurt the average)
        avg_ranks = (
            team_heat_full.groupby(["TeamID", "case"], as_index=False)["heat_rank"].mean()
                          .rename(columns={"heat_rank": "avg_rank"})
        )
        avg_ranks_wide = avg_ranks.pivot_table(index="TeamID", columns="case", values="avg_rank").reset_index()
        avg_ranks_wide = avg_ranks_wide.rename(
            columns={c: f"avg_rank_{c}" for c in avg_ranks_wide.columns if c != "TeamID"}
        )

        wide = wide.merge(avg_ranks_wide, on="TeamID", how="left")

        # Case ranks and overall rank (lower is better)
        for col in [c for c in wide.columns if c.startswith("avg_rank_")]:
            wide[f"case_rank_{col.replace('avg_rank_', '')}"] = wide[col].rank(method="min", ascending=True)

        case_cols = [c for c in wide.columns if c.startswith("case_rank_")]
        if case_cols:
            wide["average_case_rank"] = wide[case_cols].mean(axis=1, skipna=True)
            wide["overall_rank"] = wide["average_case_rank"].rank(method="min", ascending=True)
        else:
            wide["average_case_rank"] = np.nan
            wide["overall_rank"] = np.nan

        self.team_wide = wide

    # ---------- robust save ----------
    def save(self, filename: str) -> None:
        """
        Save results to Excel.
        - Handles Windows file locks (saves timestamped copy if locked)
        - Guards against too-long paths (writes to TEMP)
        - Falls back to TEMP on path errors
        """
        if self.team_wide is None or self.team_wide.empty:
            raise ValueError("No computed table to save. Call build_table() first.")

        out_file = filename if os.path.isabs(filename) else os.path.join(self.main_path, filename)
        out_file = _shorten_path(out_file)

        try:
            d = os.path.dirname(out_file)
            if d:
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass

        # Try preferred engine; fall back gracefully if not installed or locked
        try:
            self.team_wide.to_excel(out_file, index=False, engine="xlsxwriter")
            print(f"Saved {out_file}")
            return
        except ImportError:
            try:
                self.team_wide.to_excel(out_file, index=False)  # let pandas choose engine
                print(f"Saved {out_file}")
                return
            except PermissionError:
                pass
        except PermissionError:
            pass

        # If we got here, it's likely a lock → timestamped sibling
        try:
            alt_file = os.path.join(os.path.dirname(out_file) or ".", _timestamped(os.path.basename(out_file)))
            try:
                self.team_wide.to_excel(alt_file, index=False, engine="xlsxwriter")
                print(f"Target file locked. Saved a new copy instead: {alt_file}")
                return
            except ImportError:
                self.team_wide.to_excel(alt_file, index=False)
                print(f"Target file locked. Saved a new copy instead: {alt_file}")
                return
        except Exception:
            # Final fallback: TEMP
            temp_file = os.path.join(os.getenv("TEMP", "."), _timestamped(os.path.basename(out_file)))
            try:
                self.team_wide.to_excel(temp_file, index=False, engine="xlsxwriter")
            except ImportError:
                self.team_wide.to_excel(temp_file, index=False)
            print(f"Saved to TEMP due to lock/path issue: {temp_file}")


# ========================== Runner ==========================
if __name__ == "__main__":
    # Set your main directory containing subfolders with Results.xlsx
    main_path = r"C:\Users\yiming.chang\OneDrive - University of Toronto\Desktop\Yi-Ming Chang\Educational Developer\RITC\RITCxSmith 2025\Competition Session"
    analyzer = CaseRankAnalyzer(main_path)
    analyzer.load_and_prepare()
    analyzer.build_table()
    analyzer.save("RITCxSmith2025-Team_Results.xlsx")

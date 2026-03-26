#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Second_last_Teams.py
#
# Generate per-heat trader CSVs and apply NLV penalties to selected teams.
#
# Usage (example on Windows PowerShell):
#
#   python Second_last_Teams.py `
#       --root "C:\Path\To\Competition Session" `
#       --teams TEAM01 TEAM18 TEAM16
#
# This will create CSV files with columns:
#   TraderID, FirstName, LastName, NLV
# for each heat, and for the listed teams, 0-NLV traders will get
# a total team NLV of (default) -2,000,000 per heat (evenly split
# across that team's zero-NLV traders).
"""
Example usage running from command prompt:
          python Second_last_Teams.py --root "C:\Users\yiming.chang\OneDrive - University of Toronto\Desktop\Yi-Ming Chang\Educational Developer\RITC\RITCxSmith 2025\Competition Session" --teams TEAM01 TEAM18 TEAM08
"""

from __future__ import annotations

import argparse
import glob
import os
import re
from dataclasses import dataclass
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd


# ======================= path helpers =======================

def _find_result_files(root: str) -> List[str]:
    """
    Find per-heat result files under root.

    Preference:
      1) <root>/<something>/Results.xlsx
      2) If none, fallback to <root>/<something>/*.xlsx
    """
    root = os.path.abspath(root)
    files = glob.glob(os.path.join(root, "*", "Results.xlsx"))
    if not files:
        files = glob.glob(os.path.join(root, "*", "*.xlsx"))

    # ignore temp Excel files
    files = [f for f in files if not os.path.basename(f).startswith("~$")]

    if not files:
        raise FileNotFoundError(f"No .xlsx files found under {root!r}")
    return sorted(files)


def _extract_case(folder_name: str) -> str:
    """
    Rough case label, similar to your CaseRankAnalyzer:
      * if "arb" in name -> "Stats Arb" or "ETF Arb"
    You can tweak this function if you want stricter mapping.
    """
    low = folder_name.lower()
    if "arb" in low and "etf" in low:
        return "ETF Arb"
    if "arb" in low:
        return "Stats Arb"
    return folder_name  # fallback to folder name


def _extract_heat_num(folder_name: str) -> Optional[int]:
    """
    Prefer explicit 'heat N'; otherwise use the LAST integer found.

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


def _safe_case_slug(case: str) -> str:
    """Turn a case label into a safe filename slug, e.g. 'Stats Arb' -> 'StatsArb'."""
    slug = re.sub(r"[^A-Za-z0-9]+", "", case)
    return slug or "UnknownCase"


# ======================= data helpers =======================

def _coerce_money_like(s: pd.Series) -> pd.Series:
    """Coerce money-like values ($, commas, spaces, and (negatives)) to float."""
    s = s.astype(str).str.replace(r"\s+", "", regex=True)
    s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)
    s = s.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _pick_nlv_series(df: pd.DataFrame) -> pd.Series:
    """
    Select the NLV column robustly; fall back to PnL if necessary.
    Mirrors the logic from your CaseRankAnalyzer.
    """
    low = {str(c).strip().lower(): c for c in df.columns}

    if "nlv" in low:
        s = df[low["nlv"]]
    else:
        # try any column with 'nlv' in the name
        cands = [k for k in low if re.search(r"\bnlv\b", k)]
        if cands:
            s = df[low[sorted(cands, key=len)[0]]]
        else:
            # fall back to P&L
            cands = [k for k in low if re.search(r"\bp&?nl\b", k)]
            if not cands:
                raise KeyError("Could not locate NLV / PnL column in results sheet.")
            s = df[low[sorted(cands, key=len)[0]]]
    return _coerce_money_like(s)


def _pick_trader_id(df: pd.DataFrame) -> pd.Series:
    """
    Try to locate a TraderID-like column. Fallback to TeamID if necessary.

    The returned Series is string-typed.
    """
    low = {str(c).strip().lower(): c for c in df.columns}
    candidates = [
        "traderid",
        "trader id",
        "trader_id",
        "id",
        "teamid",
        "team id",
        "team_id",
    ]
    for key in candidates:
        if key in low:
            return df[low[key]].astype(str)
    # Last resort: first column
    return df.iloc[:, 0].astype(str)


def _split_name_into_first_last(df: pd.DataFrame):
    """
    Produce FirstName, LastName from typical result columns.

    Tries the following:
      * Separate 'First Name' and 'Last Name' columns
      * 'Trader Name', 'Name', 'Full Name' with 'First Last' format
    """
    low = {str(c).strip().lower(): c for c in df.columns}

    # Direct First / Last name columns
    first_candidates = ["firstname", "first name", "first_name"]
    last_candidates = ["lastname", "last name", "last_name", "surname"]

    first_col = next((low[k] for k in first_candidates if k in low), None)
    last_col = next((low[k] for k in last_candidates if k in low), None)

    if first_col is not None and last_col is not None:
        first = df[first_col].fillna("").astype(str)
        last = df[last_col].fillna("").astype(str)
        return first, last

    # Single full-name column
    full_candidates = [
        "tradername",
        "trader name",
        "name",
        "fullname",
        "full name",
    ]
    full_col = next((low[k] for k in full_candidates if k in low), None)
    if full_col is not None:
        full = df[full_col].fillna("").astype(str)
        parts = full.str.strip().str.split()
        first = parts.str[0].fillna("")
        last = parts.str[-1].fillna("")
        return first, last

    # Fallback to blank names
    n = len(df)
    return pd.Series([""] * n), pd.Series([""] * n)


def _derive_team_id_from_trader(trader_id: pd.Series) -> pd.Series:
    """
    Matching your existing convention: TeamID is the prefix of TraderID
    before the first '-'.
        e.g. 'TEAM01-TR01' -> 'TEAM01'
    """
    s = trader_id.astype(str)
    return s.str.split("-").str[0]


def _normalize_trader_frame(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    From an arbitrary Excel result sheet, construct a normalized DataFrame with:

        TraderID, FirstName, LastName, NLV, TeamID

    TeamID is derived from TraderID (prefix before '-') and is used only
    internally for applying penalties.
    """
    trader_id = _pick_trader_id(df_raw)
    first, last = _split_name_into_first_last(df_raw)
    nlv = _pick_nlv_series(df_raw)
    team_id = _derive_team_id_from_trader(trader_id)

    df_norm = pd.DataFrame(
        {
            "TraderID": trader_id,
            "FirstName": first,
            "LastName": last,
            "NLV": nlv,
            "TeamID": team_id,
        }
    )
    return df_norm


# ======================= core logic =======================

@dataclass
class PenaltyConfig:
    penalty_total: float
    teams: Sequence[str]


def _apply_penalties(df_norm: pd.DataFrame, cfg: PenaltyConfig) -> pd.DataFrame:
    """
    Apply penalties to selected teams in a normalized trader frame.

    For each TEAM in cfg.teams:
      * Identify rows with that TeamID AND NLV == 0 (within numeric tolerance)
      * If there are K such rows, set each NLV to penalty_total / K
        so the TEAM total becomes exactly penalty_total in that heat.

    Returns a new DataFrame (does not mutate the input).
    """
    df = df_norm.copy()
    if not cfg.teams:
        return df

    # force uppercase for comparison
    df["TeamID_upper"] = df["TeamID"].astype(str).str.upper()
    zero_mask = np.isclose(df["NLV"].to_numpy(dtype=float), 0.0)

    for raw_team in cfg.teams:
        team = raw_team.upper()
        mask_team = df["TeamID_upper"].eq(team) & zero_mask
        count = int(mask_team.sum())
        if count == 0:
            continue
        per_trader_penalty = cfg.penalty_total / float(count)
        df.loc[mask_team, "NLV"] = per_trader_penalty

    df = df.drop(columns=["TeamID_upper"])
    return df


def process_single_file(
    fp: str,
    cfg: PenaltyConfig,
    out_dir: str,
) -> str:
    """
    Read a single Excel result file, build normalized trader rows, apply
    penalties, and write a CSV with columns:

        TraderID, FirstName, LastName, NLV

    Returns the CSV output path.
    """
    folder = os.path.basename(os.path.dirname(fp))
    case_label = _extract_case(folder)
    heat_num = _extract_heat_num(folder)

    case_slug = _safe_case_slug(case_label)
    if heat_num is None:
        heat_slug = re.sub(r"\s+", "", folder)
        out_name = f"{case_slug}_{heat_slug}_TraderResults.csv"
    else:
        out_name = f"{case_slug}_Heat{heat_num}_TraderResults.csv"

    out_path = os.path.join(out_dir, out_name)

    # ----- load & normalize -----
    df_raw = pd.read_excel(fp, sheet_name=0)
    df_norm = _normalize_trader_frame(df_raw)

    # ----- apply penalties -----
    df_penalized = _apply_penalties(df_norm, cfg)

    # ----- export -----
    # We drop TeamID for the exported CSV; it's only used internally.
    export = df_penalized[["TraderID", "FirstName", "LastName", "NLV"]]
    export.to_csv(out_path, index=False)
    return out_path


def run(root: str, teams: Sequence[str], penalty_total: float, out_dir: Optional[str] = None) -> None:
    root = os.path.abspath(root)
    if out_dir is None:
        out_dir = os.path.join(root, "Adjusted_TraderCSVs")
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    files = _find_result_files(root)
    cfg = PenaltyConfig(penalty_total=penalty_total, teams=list(teams))

    print(f"Root directory         : {root}")
    print(f"Output CSV directory   : {out_dir}")
    print(f"Penalty per team/heat  : {cfg.penalty_total:,.0f}")
    print(f"Teams to penalize      : {', '.join(cfg.teams) if cfg.teams else '(none)'}")
    print(f"Found {len(files)} Excel result file(s).")

    for fp in files:
        rel = os.path.relpath(fp, root)
        out_csv = process_single_file(fp, cfg, out_dir)
        print(f"[OK] {rel} -> {os.path.relpath(out_csv, out_dir)}")


# ======================= CLI =======================

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate per-heat trader CSVs and apply NLV penalties to selected teams.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--root",
        required=True,
        help="Root folder containing per-heat result subfolders (e.g. one folder per heat).",
    )
    p.add_argument(
        "--teams",
        nargs="*",
        default=[],
        help="TeamIDs to penalize (e.g. TEAM01 TEAM18 TEAM16). Case-insensitive.",
    )
    p.add_argument(
        "--penalty",
        type=float,
        default=-2_000_000.0,
        help="Total penalty NLV per team per heat (distributed evenly across that team's zero-NLV traders).",
    )
    p.add_argument(
        "--out-dir",
        default=None,
        help="Output folder for generated CSVs. Default: <root>/Adjusted_TraderCSVs",
    )
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run(root=args.root, teams=args.teams, penalty_total=args.penalty, out_dir=args.out_dir)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RITC Scoring — Teams-Only, SUM per subheat (SQL-accurate) + Audit
Fixed H/S parser to correctly read files like H1SH2.csv, H1S3.csv, "Heat 2 Sub 5.csv", etc.

cd "python scripts folder path"
python ScoringPortal.py --root "Z:\lab\RITCWebsite\scoring\Previous_Competition_Results\Sample result data" --out "Final_Results.xlsx"
"""

from __future__ import annotations
import argparse
import csv
import re
from io import BytesIO, StringIO
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import numpy as np
import pandas as pd

# -------- Config --------

DEFAULT_CASE_WEIGHTS = {
    "Energy ": 20.0,
    "Merger Arbitrage": 20.0,
    "Algorithmic Market Making": 20.0,
    "Volatility": 20.0,
    "Liquidity Risk": 20.0
}

# Robust combined pattern: H<d+> ... S(H|UB|UBHEAT)?<d+>
COMBO_HS = re.compile(
    r"h\s*([0-9]+)\s*[^0-9a-zA-Z]*s(?:h|ub(?:heat)?)?\s*([0-9]+)",
    re.IGNORECASE,
)
HEAT_ONLY = re.compile(r"(?:heat|h)\s*([0-9]+)", re.IGNORECASE)
SUB_ONLY  = re.compile(r"(?:subheat|sub|sh|s)\s*([0-9]+)", re.IGNORECASE)

ENCODING_TRY = [
    "utf-8", "utf-8-sig",
    "cp1252", "latin-1", "iso-8859-1",
    "utf-16", "utf-16-le", "utf-16-be",
]
DELIMS_TRY = [",", "\t", ";", None]  # None => engine='python' autodetect

# -------- Helpers --------

def parse_weights_arg(arg: Optional[str]) -> Dict[str, float]:
    if not arg:
        return DEFAULT_CASE_WEIGHTS.copy()
    out: Dict[str, float] = {}
    for pair in arg.split(","):
        if "::" in pair:
            k, v = pair.split("::", 1)
            try:
                out[k.strip()] = float(v.strip())
            except ValueError:
                pass
    return out or DEFAULT_CASE_WEIGHTS.copy()

def case_weight_for(name: str, weights_map: Dict[str, float]) -> float:
    for key, w in weights_map.items():
        if key.lower() in name.lower():
            return float(w)
    return float(DEFAULT_CASE_WEIGHTS.get(name, 25.0))

def normalize_trader_id(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.str.replace(r'[\u200B-\u200D\uFEFF]', '', regex=True)  # zero-width/BOM
    s = s.str.strip()
    s = s.str.replace(r'\s*-\s*', '-', regex=True)
    s = s.str.upper()
    return s

def teamcode_from_traderid(series: pd.Series) -> pd.Series:
    s = normalize_trader_id(series)
    tc = s.str.split('-', n=1).str[0].str.strip()
    return tc.where(tc.str.len() > 0, "TRADERS")

def read_csv_any(path: Path, verbose: bool = True) -> pd.DataFrame:
    # Fast path
    try:
        df = pd.read_csv(path)
        if verbose:
            print(f"      ✓ {path.name} decoded as utf-8 sep=','")
        return df
    except Exception:
        pass

    data = path.read_bytes()
    last_err = None
    for enc in ENCODING_TRY:
        for sep in DELIMS_TRY:
            try:
                if enc.startswith("utf-16"):
                    if sep is None:
                        for sep2 in [",", "\t", ";"]:
                            try:
                                df = pd.read_csv(BytesIO(data), encoding=enc, sep=sep2)
                                if verbose:
                                    print(f"      ✓ {path.name} decoded as {enc} sep='{sep2}'")
                                return df
                            except Exception:
                                continue
                        continue
                    else:
                        df = pd.read_csv(BytesIO(data), encoding=enc, sep=sep)
                        if verbose:
                            print(f"      ✓ {path.name} decoded as {enc} sep='{sep}'")
                        return df
                else:
                    if sep is None:
                        df = pd.read_csv(BytesIO(data), encoding=enc, sep=None, engine="python")
                        if verbose:
                            print(f"      ✓ {path.name} decoded as {enc} sep='auto'")
                        return df
                    else:
                        df = pd.read_csv(BytesIO(data), encoding=enc, sep=sep)
                        if verbose:
                            print(f"      ✓ {path.name} decoded as {enc} sep='{sep}'")
                        return df
            except Exception as e:
                last_err = e
                continue
    # Fallback
    text = data.decode("utf-8", errors="replace")
    try:
        sniff = csv.Sniffer().sniff(text.splitlines()[0])
        sep = sniff.delimiter
    except Exception:
        sep = ","
    df = pd.read_csv(StringIO(text), sep=sep)
    if verbose:
        print(f"      ✓ {path.name} decoded via 'replace' sep='{sep}' (fallback)")
    return df

def infer_heat_sub(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Accepts: H1SH2, H1S2, Heat 1 Sub 2, H-1_S-2, etc.
    Returns (heat, sub) or (None, None) if not found.
    """
    s = str(text)
    # Try combined "H... S..." pattern first
    m = COMBO_HS.search(s)
    if m:
        return int(m.group(1)), int(m.group(2))
    # Fallback: independent searches
    mh = HEAT_ONLY.search(s)
    ms = SUB_ONLY.search(s)
    h = int(mh.group(1)) if mh else None
    sh = int(ms.group(1)) if ms else None
    return h, sh

def safe_sheet_name(name: str, prefix: str = "", used: Optional[set] = None) -> str:
    invalid = set(r'[]:*?/\\')
    base = "".join(ch for ch in name if ch not in invalid).strip()
    if prefix:
        base = f"{prefix}_{base}"
    if len(base) > 31:
        base = base[:31]
    if used is None:
        return base
    s = base
    i = 1
    while s in used:
        suffix = f"_{i}"
        s = (base[:31-len(suffix)] + suffix)
        i += 1
    used.add(s)
    return s

# -------- Scan folders → base rows (TeamCode only) --------

def scan_root(root: Path, weights_map: Dict[str, float]):
    if not root.is_dir():
        raise FileNotFoundError(f"Root folder not found: {root}")

    case_dirs = [d for d in root.iterdir() if d.is_dir()]
    if not case_dirs:
        raise RuntimeError(f"No case folders under: {root}")

    print("📂 Cases found:")
    for d in sorted(case_dirs, key=lambda p: p.name.lower()):
        print(f"  - {d.name}")

    rows: List[dict] = []
    for case_dir in sorted(case_dirs, key=lambda p: p.name.lower()):
        case_name = case_dir.name.strip()
        weight = case_weight_for(case_name, weights_map)

        files = sorted([p for p in case_dir.rglob("*.csv")], key=lambda p: str(p).lower())
        if not files:
            print(f"⚠️  {case_name}: no CSV files — skipping")
            continue

        print(f"→ {case_name}: {len(files)} file(s)")

        next_heat = 1
        for f in files:
            rel = str(f.relative_to(case_dir)).replace("\\", "/")
            ui_heat, ui_sub = infer_heat_sub(rel)
            if ui_heat is None:
                ui_heat = next_heat
                next_heat += 1
            if ui_sub is None:
                ui_sub = 1

            print(f"    · {f.name} → Heat={ui_heat}, Sub={ui_sub}")  # DEBUG: show mapping

            try:
                df = read_csv_any(f, verbose=True)
            except Exception as ex:
                print(f"      ⚠️ {f.name}: read error -> {ex} (skipped)")
                continue

            expected = ["TraderID", "FirstName", "LastName", "NLV"]
            missing = [c for c in expected if c not in df.columns]
            if missing:
                print(f"      ⚠️ {f.name}: missing columns {missing} (skipped)")
                continue

            # Normalize fields
            df["TraderID"] = normalize_trader_id(df["TraderID"])
            df["NLV"] = pd.to_numeric(df["NLV"], errors="coerce").fillna(0.0)

            # TeamCode from TraderID
            df["TeamCode"] = teamcode_from_traderid(df["TraderID"])

            # Emit base rows (Adjustment=0, Publish=1, Type=1)
            out = pd.DataFrame({
                "CaseName":  case_name,
                "CaseID":    abs(hash(case_name)) % 1_000_000,
                "HeatID":    int(ui_heat),
                "SubHeatID": int(ui_sub),
                "TeamCode":  df["TeamCode"].astype(str),
                "TraderID":  df["TraderID"].astype(str),
                "NLV":       df["NLV"].astype(float),
                "Adjustment": 0.0,
                "Weight":     float(weight),
                "Publish":    1,
                "Type":       1,
            })
            rows.extend(out.to_dict("records"))

    if not rows:
        raise RuntimeError("No rows loaded. Check folder contents and file columns.")
    return pd.DataFrame(rows)

# -------- Views (TeamCode-only, exact SQL semantics) --------

def view_AllPnLStudent(base: pd.DataFrame) -> pd.DataFrame:
    allp = base[(base["Publish"] == 1) & (base["Type"] == 1)].copy()
    allp["Profit"] = allp["NLV"] + allp["Adjustment"]
    return allp[["CaseID","CaseName","HeatID","SubHeatID","TeamCode","TraderID","Profit","Weight"]].copy()

def view_SubHeatRanksStudent(allp: pd.DataFrame) -> pd.DataFrame:
    sub = (
        allp.groupby(["CaseID","CaseName","HeatID","SubHeatID","TeamCode"], as_index=False)
            .agg(Profit=("Profit","sum"),
                 Weight=("Weight","min"))
    )

    # zeros last, then Profit DESC (SQL RANK semantics)
    sub["_zero_key"] = np.where(sub["Profit"] != 0.0, 0, 1)
    ranks = np.empty(len(sub), dtype=int)
    for _, idx in sub.groupby(["CaseID","HeatID","SubHeatID"]).groups.items():
        part = sub.loc[idx].copy()
        part.sort_values(["_zero_key","Profit"], ascending=[True, False], inplace=True)
        current = 1
        rser = pd.Series(index=part.index, dtype=int)
        for _, same in part.groupby("Profit", sort=False):
            rser.loc[same.index] = current
            current += len(same)
        ranks[idx] = rser.loc[idx].values
    sub["Rank"] = ranks.astype(int)
    sub["Score"] = sub["Rank"].astype(float)

    out = sub.drop(columns=["_zero_key"])
    out = out[[
        "Profit","TeamCode","CaseID","CaseName","HeatID","SubHeatID","Weight","Rank","Score"
    ]].sort_values(["CaseID","HeatID","SubHeatID","Rank","TeamCode"]).reset_index(drop=True)
    return out

def view_HeatRanksStudent(subheat: pd.DataFrame) -> pd.DataFrame:
    agg = (
        subheat.groupby(["CaseID","CaseName","HeatID","TeamCode"], as_index=False)
               .agg(Score=("Score","mean"),
                    Weight=("Weight","min"))
    )
    heat_ranks = np.empty(len(agg), dtype=int)
    for _, idx in agg.groupby(["CaseID","HeatID"]).groups.items():
        part = agg.loc[idx].copy()
        part.sort_values(["Score"], ascending=[True], inplace=True)
        current = 1
        rser = pd.Series(index=part.index, dtype=int)
        for _, same in part.groupby("Score", sort=False):
            rser.loc[same.index] = current
            current += len(same)
        heat_ranks[idx] = rser.loc[idx].values
    agg["Rank"] = heat_ranks.astype(int)

    return agg[[
        "TeamCode","CaseID","CaseName","HeatID","Weight","Score","Rank"
    ]].sort_values(["CaseID","HeatID","Rank","TeamCode"]).reset_index(drop=True)

def view_CaseRanksStudent(heat: pd.DataFrame, team_codes: pd.Series) -> pd.DataFrame:
    team_count = int(team_codes.nunique())
    avg_heat = (
        heat.groupby(["TeamCode","CaseID","CaseName"], as_index=False)
            .agg(AvgHeatRank=("Rank","mean"),
                 Weight=("Weight","min"))
    )
    cr_ranks = np.empty(len(avg_heat), dtype=int)
    for _, idx in avg_heat.groupby("CaseID").groups.items():
        part = avg_heat.loc[idx].copy()
        part.sort_values(["AvgHeatRank"], ascending=[True], inplace=True)
        current = 1
        rser = pd.Series(index=part.index, dtype=int)
        for _, same in part.groupby("AvgHeatRank", sort=False):
            rser.loc[same.index] = current
            current += len(same)
        cr_ranks[idx] = rser.loc[idx].values
    avg_heat["Rank"] = cr_ranks.astype(int)
    avg_heat["Score"] = team_count - avg_heat["Rank"] + 1
    return avg_heat[[
        "TeamCode","CaseID","CaseName","Weight","Score","Rank"
    ]].sort_values(["CaseID","Rank","TeamCode"]).reset_index(drop=True)


def view_TotalRanksStudent(case_ranks: pd.DataFrame) -> pd.DataFrame:
    cr = case_ranks.copy()
    # Calculate weighted score per case
    cr["Weighted"] = cr["Score"] * (cr["Weight"] / 100.0)
    
    # Aggregate Score and Variance
    totals = (
        cr.groupby("TeamCode", as_index=False)
          .agg(Score=("Weighted","sum"),
               # If < 2 cases, Variance is 0.0
               Var=("Score", lambda s: float(pd.Series(s).var(ddof=1)) if len(s) >= 2 else 0.0))
    )

    # Prepare for sorting:
    # 1. High Score is better (negate it for sorting)
    # 2. Low Variance is better (keep positive)
    # 3. TeamCode alphabetical (tie-breaker)
    
    # We use a temporary dataframe to sort easily
    totals["NegScore"] = -totals["Score"]
    
    # Sort strictly
    totals = totals.sort_values(by=["NegScore", "Var", "TeamCode"], ascending=[True, True, True])
    
    # Assign strictly unique ranks (1, 2, 3, 4...)
    totals["Rank"] = range(1, len(totals) + 1)
    
    # Clean up and return
    return totals[["TeamCode", "Score", "Var", "Rank"]].reset_index(drop=True)
# def view_TotalRanksStudent(case_ranks: pd.DataFrame) -> pd.DataFrame:
#     cr = case_ranks.copy()
#     cr["Weighted"] = cr["Score"] * (cr["Weight"] / 100.0)
#     totals = (
#         cr.groupby("TeamCode", as_index=False)
#           .agg(Score=("Weighted","sum"),
#                Var=("Score", lambda s: float(pd.Series(s).var(ddof=1)) if len(s) >= 2 else 0.0))
#     )
#     var_for_rank = totals["Var"].fillna(np.inf)
#     order = np.lexsort((var_for_rank.values, -totals["Score"].values))
#     key = list(zip(totals["Score"].round(12), var_for_rank.round(12)))
#     rank_map: Dict[Tuple[float,float], int] = {}
#     cur = 1
#     for idx in order:
#         k = key[idx]
#         if k not in rank_map:
#             rank_map[k] = cur
#         cur += 1
#     totals["Rank"] = [rank_map[k] for k in key]
#     return totals[["TeamCode","Score","Var","Rank"]].sort_values(["Rank","TeamCode"]).reset_index(drop=True)

# -------- Extra: pivots + audit --------

def build_case_pivots(subheat: pd.DataFrame) -> Dict[str, Dict[str, pd.DataFrame]]:
    out: Dict[str, Dict[str, pd.DataFrame]] = {}
    sub = subheat.copy()
    sub["H_S"] = sub.apply(lambda r: f"H{int(r.HeatID)}S{int(r.SubHeatID)}", axis=1)
    for case_name, g in sub.groupby("CaseName"):
        pnl_piv = g.pivot_table(index="TeamCode", columns="H_S", values="Profit", aggfunc="first")
        rnk_piv = g.pivot_table(index="TeamCode", columns="H_S", values="Rank",   aggfunc="first")
        cols_sorted = sorted(pnl_piv.columns, key=lambda k: (int(k.split('S')[0][1:]), int(k.split('S')[1])))
        pnl_piv = pnl_piv.reindex(columns=cols_sorted).sort_index()
        rnk_piv = rnk_piv.reindex(columns=cols_sorted).sort_index()
        out[case_name] = {"PnL": pnl_piv, "Rank": rnk_piv}
    return out

def build_audit(allp: pd.DataFrame, subheat: pd.DataFrame) -> pd.DataFrame:
    raw = (
        allp.groupby(["CaseID","CaseName","HeatID","SubHeatID","TeamCode"], as_index=False)
            .agg(RawTeamSum=("Profit","sum"))
    )
    merged = raw.merge(
        subheat[["CaseID","CaseName","HeatID","SubHeatID","TeamCode","Profit","Rank"]],
        on=["CaseID","CaseName","HeatID","SubHeatID","TeamCode"],
        how="left"
    )
    merged["Delta"] = (merged["RawTeamSum"] - merged["Profit"]).round(10)
    return merged.sort_values(["CaseID","HeatID","SubHeatID","TeamCode"]).reset_index(drop=True)

# -------- Pipeline --------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Root folder containing per-case subfolders")
    ap.add_argument("--out",  default="ScoresCheck_Teams.xlsx", help="Output Excel workbook")
    ap.add_argument("--weights", default=None,
                    help='Optional mapping: "Commodities::25,ETF::25,Fixed Income::25,Volatility::25" (substring match)')
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    weights_map = parse_weights_arg(args.weights)

    base = scan_root(root, weights_map)
    print(f"\n✅ Loaded trader rows: {len(base)} | Cases: {base['CaseID'].nunique()} | Teams: {base['TeamCode'].nunique()}")

    allp = view_AllPnLStudent(base)
    sub  = view_SubHeatRanksStudent(allp)   # team SUM per subheat
    heat = view_HeatRanksStudent(sub)
    case = view_CaseRanksStudent(heat, team_codes=base["TeamCode"])
    total= view_TotalRanksStudent(case)

    pivots = build_case_pivots(sub)
    audit  = build_audit(allp, sub)

    used_names = set()
    with pd.ExcelWriter(args.out, engine="openpyxl") as xl:
        sub.to_excel(xl,   index=False, sheet_name="SubHeatRanksStudent")
        heat.to_excel(xl,  index=False, sheet_name="HeatRanksStudent")
        case.to_excel(xl,  index=False, sheet_name="CaseRanksStudent")
        total.to_excel(xl, index=False, sheet_name="TotalRanksStudent")
        for case_name, d in pivots.items():
            pnl_sheet  = safe_sheet_name(case_name, prefix="PnL",  used=used_names)
            rank_sheet = safe_sheet_name(case_name, prefix="Rank", used=used_names)
            d["PnL"].to_excel(xl,  sheet_name=pnl_sheet)
            d["Rank"].to_excel(xl, sheet_name=rank_sheet)
        audit.to_excel(xl, index=False, sheet_name="Audit_SubheatTeamSum")

    print(f"📄 Wrote: {args.out}")

if __name__ == "__main__":
    main()

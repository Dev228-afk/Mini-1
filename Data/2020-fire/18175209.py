#!/usr/bin/env python3
# merge_csvs.py
# Generic recursive CSV merger with optional AirNow schema handling.

import argparse
import csv
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

AIRNOW_COLS = [
    "Latitude", "Longitude", "UTC", "Parameter", "Concentration", "Unit",
    "Raw Concentration", "AQI", "Category", "Site Name", "Site Agency",
    "AQS ID", "Full AQS ID",
]

AIRNOW_NUMERIC = {"Latitude", "Longitude", "Concentration", "Raw Concentration", "AQI", "Category"}

def has_header_quick(path: Path) -> bool:
    """Heuristic: if first line has letters/known headers, assume a header."""
    with path.open("r", newline="", encoding="utf-8") as f:
        first = f.readline().strip()
    # crude but practical: look for alphabetic chars or known column tokens
    return any(ch.isalpha() for ch in first) and any(k in first for k in ["Lat", "UTC", "Parameter", "AQI", "Site"])

def count_fields(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        row = next(r, [])
        return len(row)

def read_one_csv(
    path: Path,
    base_dir: Path,
    schema: str,
    assume_header: Optional[bool],
    sentinel: Optional[float],
    chunksize: Optional[int],
) -> pd.DataFrame:
    # Decide header mode
    header_mode: Optional[int] = 0  # header row index or None
    names: Optional[List[str]] = None

    if schema == "airnow":
        # Default: headerless AirNow exports
        if assume_header is True:
            header_mode = 0
            names = None
        elif assume_header is False:
            header_mode = None
            names = AIRNOW_COLS
        else:
            # auto detect
            header_mode = 0 if has_header_quick(path) else None
            names = None if header_mode == 0 else AIRNOW_COLS
    elif schema == "infer":
        header_mode = 0 if (assume_header is not False) else None
        if header_mode is None:
            # Create generic names col_0..col_n-1
            n = count_fields(path)
            names = [f"col_{i}" for i in range(n)]
    else:
        raise ValueError(f"Unknown schema: {schema}")

    def _postprocess(df: pd.DataFrame) -> pd.DataFrame:
        # Parse UTC if present
        if "UTC" in df.columns:
            df["UTC"] = pd.to_datetime(df["UTC"], errors="coerce")
        # Cast numerics (AirNow known numerics)
        for col in (set(df.columns) & AIRNOW_NUMERIC):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        # Sentinel → NA
        if sentinel is not None:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df.loc[df[col] == sentinel, col] = pd.NA
        # Provenance
        df["__source_file"] = path.name
        df["__source_dir"] = str(path.parent.relative_to(base_dir)) if path.parent != base_dir else "."
        df["__source_path"] = str(path.relative_to(base_dir))
        return df

    if chunksize:
        # streaming read
        frames = []
        for chunk in pd.read_csv(
            path,
            header=header_mode,
            names=names,
            dtype=str if schema == "airnow" else None,
            na_values=["", "NA", "N/A"],
            keep_default_na=True,
            encoding="utf-8",
            chunksize=chunksize,
        ):
            frames.append(_postprocess(chunk))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    else:
        df = pd.read_csv(
            path,
            header=header_mode,
            names=names,
            dtype=str if schema == "airnow" else None,
            na_values=["", "NA", "N/A"],
            keep_default_na=True,
            encoding="utf-8",
        )
        return _postprocess(df)

def main():
    ap = argparse.ArgumentParser(description="Recursively merge CSVs into one dataset.")
    ap.add_argument("base_dir", type=Path, help="Top-level folder containing subfolders with CSVs")
    ap.add_argument("--pattern", default="*.csv", help="Glob for CSVs (default: *.csv)")
    ap.add_argument("--schema", choices=["airnow", "infer"], default="airnow",
                    help="airnow: use AirNow column schema (default). infer: use file header or generic cols")
    ap.add_argument("--assume-header", choices=["true", "false", "auto"], default="auto",
                    help="Treat files as having a header row. auto tries to detect. default: auto")
    ap.add_argument("--sentinel", type=float, default=-999,
                    help="Numeric sentinel to treat as missing (default: -999). Use --sentinel NaN to disable.")
    ap.add_argument("--chunksize", type=int, default=None,
                    help="Read CSVs in chunks (e.g., 100000) for large files")
    ap.add_argument("--dedupe-keys", nargs="*", default=["UTC", "Latitude", "Longitude", "Parameter", "AQS ID"],
                    help="Keys for soft de-duplication (ignored if missing)")
    ap.add_argument("--sort-by", nargs="*", default=["UTC", "Site Name", "Parameter"],
                    help="Sort columns if present")
    ap.add_argument("--out-csv", type=Path, default=None, help="Output CSV path (default: <base_dir>/merged.csv)")
    ap.add_argument("--out-parquet", type=Path, default=None,
                    help="Optional Parquet path (requires pyarrow or fastparquet).")
    args = ap.parse_args()

    base = args.base_dir.resolve()
    if not base.exists():
        print(f"Base dir not found: {base}", file=sys.stderr)
        sys.exit(1)

    # sentinel handling
    sentinel_val = None if (str(args.sentinel).lower() == "nan") else args.sentinel

    # find all CSVs
    files = sorted(
        p for p in base.rglob(args.pattern)
        if p.is_file() and p.name not in {"merged.csv", "merged.parquet"}
    )
    if not files:
        print("No CSV files found.", file=sys.stderr)
        sys.exit(2)

    # header mode
    assume_header = {"true": True, "false": False, "auto": None}[args.assume_header]

    frames = []
    for p in files:
        try:
            frames.append(read_one_csv(p, base, args.schema, assume_header, sentinel_val, args.chunksize))
        except Exception as e:
            print(f"⚠️ Skipping {p}: {e}", file=sys.stderr)

    if not frames:
        print("No CSVs could be read successfully.", file=sys.stderr)
        sys.exit(3)

    merged = pd.concat(frames, ignore_index=True)

    # strict de-dup
    merged = merged.drop_duplicates()

    # soft de-dup on available keys
    soft_keys = [k for k in args.dedupe_keys if k in merged.columns]
    if soft_keys:
        merged = (merged
                  .sort_values([c for c in args.sort_by if c in merged.columns])
                  .drop_duplicates(subset=soft_keys, keep="first"))

    # outputs
    out_csv = args.out_csv or (base / "merged.csv")
    merged.to_csv(out_csv, index=False)
    print(f"✓ Wrote CSV: {out_csv}")

    if args.out_parquet:
        try:
            merged.to_parquet(args.out_parquet, index=False)
            print(f"✓ Wrote Parquet: {args.out_parquet}")
        except Exception as e:
            print(f"Note: could not write Parquet ({e}). Install pyarrow or fastparquet.", file=sys.stderr)

    # quick summary
    date_min = merged["UTC"].min().isoformat() if "UTC" in merged.columns and pd.notnull(merged["UTC"]).any() else None
    date_max = merged["UTC"].max().isoformat() if "UTC" in merged.columns and pd.notnull(merged["UTC"]).any() else None
    print("Rows:", len(merged))
    print("Columns:", len(merged.columns))
    if date_min or date_max:
        print("UTC range:", date_min, "→", date_max)

if __name__ == "__main__":
    main()

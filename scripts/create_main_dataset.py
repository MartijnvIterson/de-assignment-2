#!/usr/bin/env python3
"""Create a main dataset by sampling N random rows from a large CSV file.

This script uses reservoir sampling to pick ``sample_size`` rows while
streaming the input file. It is intentionally lightweight (no pandas)
so it can run with low memory usage.

Usage examples:
    python scripts/create_main_dataset.py \
        --input ../data/raw/flights.csv \
        --output ../data/raw/flights_main.csv \
        --sample-size 50000

Options:
    --input         Input CSV file path (required)
    --output        Output CSV file path (required)
    --sample-size   Number of rows to sample (default: 50000)
    --seed          Optional random seed for reproducibility
    --compress      Write output as gzip when set (flag)
"""
from __future__ import annotations

import argparse
import csv
import gzip
import random
import sys
from typing import List


def reservoir_sample_csv(inpath: str, sample_size: int, seed: int | None = None):
    """Yield header and a list of sampled rows (as lists of strings).

    Uses reservoir sampling to select `sample_size` rows from the CSV at `inpath`.
    The header (first row) is returned separately.
    """
    if seed is not None:
        random.seed(seed)

    open_func = gzip.open if inpath.endswith(".gz") else open
    reservoir: List[List[str]] = []
    header = None
    total = 0

    with open_func(inpath, mode="rt", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Input CSV is empty")

        for row in reader:
            if total < sample_size:
                reservoir.append(row)
            else:
                # choose an index in [0, total]
                j = random.randint(0, total)
                if j < sample_size:
                    reservoir[j] = row

            total += 1
            # occasional progress to stderr (non-verbose): every 1M rows
            if total % 1_000_000 == 0:
                print(f"Read {total:,} rows...", file=sys.stderr)

    # If dataset smaller than sample_size, reservoir contains all rows
    return header, reservoir


def write_csv(outpath: str, header: List[str], rows: List[List[str]], compress: bool = False) -> None:
    open_func = gzip.open if (compress or outpath.endswith(".gz")) else open
    mode = "wt"
    with open_func(outpath, mode=mode, encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def parse_args():
    p = argparse.ArgumentParser(description="Sample a main dataset from a large CSV with reservoir sampling")
    p.add_argument("--input", required=True, help="Path to input CSV (can be .gz)")
    p.add_argument("--output", required=True, help="Path to output CSV (will be overwritten)")
    p.add_argument("--sample-size", type=int, default=50000, help="Number of rows to sample (default 50000)")
    p.add_argument("--seed", type=int, default=None, help="Optional random seed for reproducibility")
    p.add_argument("--compress", action="store_true", help="Write output as gzip (.gz) regardless of output extension")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Sampling {args.sample_size:,} rows from {args.input} ...", file=sys.stderr)
    header, sample_rows = reservoir_sample_csv(args.input, args.sample_size, seed=args.seed)
    print(f"Selected {len(sample_rows):,} rows (input may have fewer rows than requested). Writing to {args.output}", file=sys.stderr)
    write_csv(args.output, header, sample_rows, compress=args.compress)
    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()

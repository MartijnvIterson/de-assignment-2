#!/usr/bin/env python3
"""
Split flights.csv in kleinere chunks voor streaming-simulatie.

Voorbeeld:
    python split_flights_for_streaming.py \
        --input ../data/raw/flights.csv \
        --output-dir ./flights_chunks \
        --rows-per-file 50000 \
        --max-files 10
"""

import argparse
import os
import pandas as pd


def split_csv(input_path, output_dir, rows_per_file=50000, max_files=None):
    os.makedirs(output_dir, exist_ok=True)

    reader = pd.read_csv(input_path, chunksize=rows_per_file)
    for i, chunk in enumerate(reader, start=1):
        if max_files is not None and i > max_files:
            break
        out_path = os.path.join(output_dir, f"flights_part_{i:03d}.csv")
        chunk.to_csv(out_path, index=False)
        print(f"Wrote {len(chunk)} rows to {out_path}")

    print("Done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to flights.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for chunk files")
    parser.add_argument("--rows-per-file", type=int, default=50000)
    parser.add_argument("--max-files", type=int, default=None)
    args = parser.parse_args()

    split_csv(args.input, args.output_dir, args.rows_per_file, args.max_files)


if __name__ == "__main__":
    main()

"""Split the PaySim CSV into daily batch files and a held-back streaming file.

Batch:  steps 1-696 (days 1-29), one CSV per day, loaded by ADF in Phase 2.
Stream: steps 697-743 (the last ~2 days), one CSV, replayed through Event Hubs in Phase 4.
"""
import argparse
from pathlib import Path

import pandas as pd

HOURS_PER_DAY = 24
BATCH_LAST_STEP = 696  # last hour of day 29


def split_paysim(input_path: Path, output_dir: Path) -> None:
    df = pd.read_csv(input_path, engine="pyarrow")
    total = len(df)

    day = (df["step"] - 1) // HOURS_PER_DAY + 1
    batch = df[df["step"] <= BATCH_LAST_STEP]
    stream = df[df["step"] > BATCH_LAST_STEP]

    written = 0
    landing = output_dir / "landing" / "paysim"
    for d, day_df in batch.groupby(day.loc[batch.index]):
        day_dir = landing / f"day={d:02d}"
        day_dir.mkdir(parents=True, exist_ok=True)
        day_df.to_csv(day_dir / f"paysim_day{d:02d}.csv", index=False)
        written += len(day_df)
        print(f"day {d:02d}: {len(day_df):>8,} rows")

    stream_dir = output_dir / "stream"
    stream_dir.mkdir(parents=True, exist_ok=True)
    stream.to_csv(stream_dir / "paysim_stream.csv", index=False)
    written += len(stream)
    print(f"stream: {len(stream):,} rows (steps {stream['step'].min()}-{stream['step'].max()})")

    if written != total:
        raise ValueError(f"Row count mismatch: input {total:,}, written {written:,}")
    print(f"OK: {total:,} input rows = {written:,} rows written")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split PaySim into daily batch files and a streaming file.")
    parser.add_argument("--input", type=Path, default=Path("data/raw/paysim.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    split_paysim(args.input, args.output_dir)
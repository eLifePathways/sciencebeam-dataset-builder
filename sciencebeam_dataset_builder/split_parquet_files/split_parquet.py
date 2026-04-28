import argparse
import os
import pandas as pd
from sklearn.model_selection import train_test_split


def split_parquet_files(input_dir: str, output_dir: str) -> None:
    parquet_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.endswith(".parquet")
    ]
    if not parquet_files:
        raise ValueError(f"No parquet files found in {input_dir}")

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    train_df, temp_df = train_test_split(df, test_size=0.80, random_state=42)
    validation_df, test_df = train_test_split(temp_df, test_size=0.625, random_state=42)
    # train=20%, validation=30%, test=50%

    os.makedirs(output_dir, exist_ok=True)

    splits = {
        "train-00000-of-00001.parquet": train_df,
        "validation-00000-of-00001.parquet": validation_df,
        "test-00000-of-00001.parquet": test_df,
    }

    for filename, split_df in splits.items():
        out_path = os.path.join(output_dir, filename)
        split_df.to_parquet(out_path, index=False)
        print(f"Written {len(split_df)} rows to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Split parquet files into train/validation/test sets")
    parser.add_argument("--input-dir", required=True, help="Directory containing input parquet files")
    parser.add_argument("--output-dir", required=True, help="Directory to write split parquet files")
    args = parser.parse_args()

    split_parquet_files(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()

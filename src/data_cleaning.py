import os
import pandas as pd


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load the raw sales CSV file into a pandas DataFrame.

    file_path: path to the CSV file (can be absolute or relative).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find raw data file: {file_path}")
    df = pd.read_csv(file_path)
    return df


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names:
    - strip whitespace
    - lowercase
    - replace spaces with underscores
    """
    df = df.copy()
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip leading/trailing whitespace from common text columns if they exist.
    """
    df = df.copy()
    for col in ("prodname", "category"):
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 'price' and 'qty' to numeric (coerce errors) and drop rows missing
    price, qty, or date_sold if those columns exist.
    """
    df = df.copy()
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "qty" in df.columns:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    required = [c for c in ("price", "qty", "date_sold") if c in df.columns]
    if required:
        df = df.dropna(subset=required)
    return df


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with negative price or negative quantity.
    Keep rows with price >= 0 and qty >= 0 (change to > 0 if you prefer).
    """
    df = df.copy()
    if "price" in df.columns:
        df = df[df["price"] >= 0]
    if "qty" in df.columns:
        df = df[df["qty"] >= 0]
    return df


def main():
    # Build paths relative to the repository root (parent of src/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))
    raw_path = os.path.join(repo_root, "data", "raw", "sales_data_raw.csv")
    cleaned_path = os.path.join(repo_root, "data", "processed", "sales_data_clean.csv")

    print("Using raw_path:", raw_path)
    if not os.path.exists(raw_path):
        raise FileNotFoundError(
            f"Raw data not found at {raw_path}. Make sure the file exists and you run the script from the repository."
        )

    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = strip_whitespace(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)

    os.makedirs(os.path.dirname(cleaned_path), exist_ok=True)
    df_clean.to_csv(cleaned_path, index=False)

    print("Cleaning complete. First few rows:")
    print(df_clean.head())

"""
data_cleaning.py
----------------
This script loads a raw sales dataset and performs cleaning steps such as:
- standardizing column names
- trimming whitespace from text fields
- fixing inconsistent product/category formatting
- handling missing or invalid prices and quantities
- removing rows with clearly invalid values
The cleaned output is saved into data/processed/sales_data_clean.csv.
"""

import os
import pandas as pd

# ---------------------------------------------------------
# Copilot-assisted function #1 — load data
# ---------------------------------------------------------
# Copilot generated the basic read_csv code, but I added
# error handling and print messages.
def load_data(file_path: str) -> pd.DataFrame:
    """Load the sales CSV or Excel file.

    - Accepts .csv, .csv.gz, .xls, .xlsx files.
    - Raises FileNotFoundError if path does not exist.
    - Raises ValueError for empty files.
    - Wraps other read errors in RuntimeError for clarity.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Could not find raw data file: {file_path}")

    try:
        lower = file_path.lower()
        if lower.endswith((".xls", ".xlsx")):
            df = pd.read_excel(file_path)
        else:
            # pandas can handle .csv and compressed csv like .csv.gz automatically
            df = pd.read_csv(file_path)
        print(f"Loaded file: {file_path} (rows: {len(df):,}, cols: {len(df.columns)})")
        return df
    except pd.errors.EmptyDataError:
        raise ValueError(f"No data: the file '{file_path}' appears to be empty.")
    except Exception as exc:
        # Re-raise with more context while preserving the original exception
        raise RuntimeError(f"Failed to read '{file_path}': {exc}") from exc


# ---------------------------------------------------------
# Copilot-assisted function #2 — clean column names
# ---------------------------------------------------------
# Copilot suggested lowercase + replacing spaces.
# I modified it to also strip whitespace and ensure
# consistent underscores.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names (lowercase, underscores)."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


# ---------------------------------------------------------
# Strip whitespace from text columns
# ---------------------------------------------------------
def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace in all string (object) columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().str.replace('"', "")
    return df


# ---------------------------------------------------------
# Fix misplaced or missing numeric values
# ---------------------------------------------------------
# Your dataset contains:
# - "-" instead of numbers
# - swapped price/qty values
# - missing dates
def fix_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert price and qty to numeric and fix '-' entries."""
    
    # Replace "-" with NaN
    if "price" in df.columns:
        df["price"].replace("-", pd.NA, inplace=True)
    if "qty" in df.columns:
        df["qty"].replace("-", pd.NA, inplace=True)

    # Convert to numeric
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
    if "qty" in df.columns:
        df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    return df


# ---------------------------------------------------------
# Handle missing values
# ---------------------------------------------------------
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows missing price, qty, or date."""
    required = [c for c in ("price", "qty", "date_sold") if c in df.columns]
    if required:
        df = df.dropna(subset=required)
    return df


# ---------------------------------------------------------
# Remove invalid rows
# ---------------------------------------------------------
# Rules:
# - negative quantities NOT allowed
# - price must be > 0
# - qty must be > 0
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with invalid price/qty values and correct common mistakes.

    Steps performed:
    - Ensure 'price' and 'qty' exist and are numeric (coerce errors to NaN).
    - Heuristic swap correction: if price looks like a small value (<1)
      and qty looks like a large integer (>100), swap them (common CSV mixups).
      The heuristic is conservative to avoid accidental swaps.
    - Drop rows where price or qty are missing or <= 0.
    - Remove implausible extreme outliers (price or qty > 1_000_000).
    - Drop exact duplicate rows.

    Returns a cleaned DataFrame and prints a short summary of actions taken.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    df = df.copy()
    initial_count = len(df)

    # Ensure numeric columns exist
    if "price" not in df.columns or "qty" not in df.columns:
        raise KeyError("Required columns 'price' and 'qty' are not both present in the DataFrame")

    # Coerce to numeric (preserve previous NaN behavior)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    # Heuristic: detect and fix swapped price/qty values
    # Very conservative rule: price < 1, qty >= 100, and qty >> price
    swap_mask = (df["price"].notna()) & (df["qty"].notna()) & (df["price"] < 1) & (df["qty"] >= 100) & (df["qty"] > df["price"] * 10)
    swapped = int(swap_mask.sum())
    if swapped:
        # swap values for detected rows
        df.loc[swap_mask, ["price", "qty"]] = df.loc[swap_mask, ["qty", "price"]].values
        print(f"Swapped price/qty in {swapped} rows based on heuristic.")

    # Drop rows with missing or non-positive price/qty
    mask_valid = (df["price"] > 0) & (df["qty"] > 0)
    removed_missing_or_nonpositive = int((~mask_valid).sum())
    df = df[mask_valid]

    # Remove implausible extreme outliers
    outlier_mask = (df["price"] > 1_000_000) | (df["qty"] > 1_000_000)
    removed_outliers = int(outlier_mask.sum())
    if removed_outliers:
        df = df[~outlier_mask]
        print(f"Removed {removed_outliers} rows with implausible values (> 1,000,000).")

    # Drop exact duplicates
    before_dup = len(df)
    df = df.drop_duplicates()
    dup_removed = before_dup - len(df)

    final_count = len(df)
    print(
        f"remove_invalid_rows: start={initial_count}, swapped={swapped}, "
        f"removed_missing_or_nonpositive={removed_missing_or_nonpositive}, "
        f"duplicates_removed={dup_removed}, final={final_count}"
    )

    return df


# ---------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------
if __name__ == "__main__":

    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = strip_whitespace(df_clean)
    df_clean = fix_numeric_values(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)

    os.makedirs(os.path.dirname(cleaned_path), exist_ok=True)
    df_clean.to_csv(cleaned_path, index=False)

    print("\nCleaning complete! First 5 rows:")
    print(df_clean.head())

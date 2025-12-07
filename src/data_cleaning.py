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

import pandas as pd

# ---------------------------------------------------------
# Copilot-assisted function #1 — load data
# ---------------------------------------------------------
# Copilot generated the basic read_csv code, but I added
# error handling and print messages.
def load_data(file_path: str) -> pd.DataFrame:
    """Load the sales CSV file."""
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded file: {file_path}")
        return df
    except FileNotFoundError:
        raise FileNotFoundError("Could not find raw data file.")


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
    df["price"].replace("-", pd.NA, inplace=True)
    df["qty"].replace("-", pd.NA, inplace=True)

    # Convert to numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")

    return df


# ---------------------------------------------------------
# Handle missing values
# ---------------------------------------------------------
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows missing price, qty, or date."""
    df = df.dropna(subset=["price", "qty", "date_sold"])
    return df


# ---------------------------------------------------------
# Remove invalid rows
# ---------------------------------------------------------
# Rules:
# - negative quantities NOT allowed
# - price must be > 0
# - qty must be > 0
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with invalid price/qty values."""
    df = df[(df["price"] > 0) & (df["qty"] > 0)]
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

    df_clean.to_csv(cleaned_path, index=False)

    print("\nCleaning complete! First 5 rows:")
    print(df_clean.head())

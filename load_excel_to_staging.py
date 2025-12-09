import argparse
import pandas as pd
import pyodbc
import os
import re

# ----------------------------
# SQL SERVER CONNECTION
# ----------------------------

def get_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-8K8UGGP\\SQLEXPRESS;"
        "DATABASE=ORDER_DDS;"
        "Trusted_Connection=yes;"
    )


# ----------------------------
# CLEANING FUNCTIONS
# ----------------------------

def clean_dataframe(df):
    """
    Force all columns to text, drop problematic columns, clean .0 floats,
    clean dates, remove NaN.
    """

    # Convert everything to string
    df = df.astype(str)

    # Replace NaN/"nan"/"None" values with empty string
    df = df.replace("nan", "").replace("None", "").fillna("")

    # Remove pandas float tails:  "5.0" → "5"
    for col in df.columns:
        df[col] = df[col].str.replace(r"\.0$", "", regex=True)

    # Drop long-text columns
    df = df.drop(columns=["Notes", "PhotoPath"], errors="ignore")

    # Clean date formats (optional)
    date_cols = ["OrderDate", "RequiredDate", "ShippedDate", 
                 "BirthDate", "HireDate"]

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_date)

    return df


def clean_date(value):
    """Attempts to convert any date-like value to yyyy-mm-dd, else empty string."""
    value = str(value).strip()

    # Empty or placeholder values
    if value in ["", "0", "0000-00-00", "NaT"]:
        return ""

    # Already correct ISO format
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value

    # Try parsing common US/EU date formats
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return ""
        return parsed.strftime("%Y-%m-%d")
    except:
        return ""


# ----------------------------
# LOAD A SINGLE SHEET INTO A STAGING TABLE
# ----------------------------

def load_sheet(cursor, df, sheet_name):
    staging_table = f"staging.{sheet_name}"

    print(f"➡️ Loading sheet '{sheet_name}' into {staging_table}...")

    if df.empty:
        print(f"⚠️ Sheet '{sheet_name}' is empty — skipping.")
        return

    df = clean_dataframe(df)

    # Build INSERT SQL dynamically
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {staging_table} ({cols}) VALUES ({placeholders})"

    # Convert dataframe rows to list of tuples
    data = list(df.itertuples(index=False, name=None))

    try:
        cursor.executemany(sql, data)
        print(f"   ✔ Loaded {len(df)} rows into {staging_table}")
    except Exception as e:
        print("\n❌ SQL Insert Error!")
        print("SQL:", sql)
        print("Sample row:", data[0] if data else "NO DATA")
        raise e


# ----------------------------
# MAIN FUNCTION TO LOAD ALL SHEETS
# ----------------------------

def load_excel_to_staging(file_path):

    if not os.path.exists(file_path):
        print(f"❌ ERROR: File not found: {file_path}")
        return

    print(f"📄 Loading Excel file: {file_path}")

    conn = get_connection()
    cursor = conn.cursor()

    # Clear staging tables
    staging_tables = [
        "Orders", "OrderDetails", "Products", "Customers",
        "Employees", "Region", "Shippers", "Suppliers", "Territories"
    ]

    print("🧹 Clearing staging tables...")
    for table in staging_tables:
        cursor.execute(f"DELETE FROM staging.{table}")
        conn.commit()

    # Load Excel with all sheets
    excel_file = pd.ExcelFile(file_path)

    for sheet in excel_file.sheet_names:
        df = excel_file.parse(sheet)
        load_sheet(cursor, df, sheet)

    conn.commit()
    conn.close()

    print("\n✅ Excel data loaded successfully!")
    print("➡️ Next: run the pipeline:")
    print("   python main.py --start_date 1996-01-01 --end_date 1996-12-31")


# ----------------------------
# COMMAND LINE ENTRY
# ----------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Excel data into SQL staging tables.")
    parser.add_argument("--excel_file", required=True, help="Path to Excel file")
    args = parser.parse_args()

    load_excel_to_staging(args.excel_file)

"""
Script to load data from raw_data.xlsx into staging tables.

This script reads Excel data and loads it into the staging schema tables
that the dimensional pipeline expects.
"""

import pandas as pd
import pyodbc
from utils import get_sql_config, create_connection_string
from pipeline_dimensional_data import config

# Mapping of Excel sheet names to staging table names
SHEET_TO_TABLE = {
    "Categories": "staging.Categories",
    "Customers": "staging.Customers",
    "Employees": "staging.Employees",
    "Products": "staging.Products",
    "Region": "staging.Region",
    "Shippers": "staging.Shippers",
    "Suppliers": "staging.Suppliers",
    "Territories": "staging.Territories",
    "Orders": "staging.Orders",
    "OrderDetails": "staging.OrderDetails",
}


def load_excel_to_staging(excel_file: str = "raw_data.xlsx", clear_existing: bool = True):
    """
    Load data from Excel file into staging tables.
    
    Args:
        excel_file: Path to the Excel file
        clear_existing: If True, truncate staging tables before loading
    """
    # Read Excel file
    try:
        excel_data = pd.read_excel(excel_file, sheet_name=None)
        print(f"✓ Loaded Excel file: {excel_file}")
        print(f"  Found {len(excel_data)} sheets")
    except FileNotFoundError:
        print(f"✗ Error: File not found: {excel_file}")
        return False
    except Exception as e:
        print(f"✗ Error reading Excel file: {e}")
        return False
    
    # Connect to database
    try:
        cfg = get_sql_config(config.SQL_CFG_FILE, config.SQL_CFG_SECTION)
        conn_str = create_connection_string(cfg)
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        print(f"✓ Connected to database: {cfg['Database']}")
    except Exception as e:
        print(f"✗ Error connecting to database: {e}")
        return False
    
    try:
        # Clear existing data if requested
        if clear_existing:
            print("\nClearing existing staging data...")
            for table in SHEET_TO_TABLE.values():
                try:
                    cursor.execute(f"TRUNCATE TABLE {table}")
                    print(f"  ✓ Cleared {table}")
                except Exception as e:
                    print(f"  ⚠ Could not clear {table}: {e}")
            conn.commit()
        
        # Load each sheet
        print("\nLoading data from Excel sheets...")
        for sheet_name, df in excel_data.items():
            if sheet_name not in SHEET_TO_TABLE:
                print(f"  ⚠ Skipping sheet '{sheet_name}' (no matching staging table)")
                continue
            
            table_name = SHEET_TO_TABLE[sheet_name]
            
            # Prepare data for insertion (exclude staging_raw_id_sk as it's IDENTITY)
            df_clean = df.copy()
            if 'staging_raw_id_sk' in df_clean.columns:
                df_clean = df_clean.drop(columns=['staging_raw_id_sk'])
            
            # Get column names
            columns = ', '.join(df_clean.columns)
            placeholders = ', '.join(['?' for _ in df_clean.columns])
            
            # Insert data
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            rows_inserted = 0
            for _, row in df_clean.iterrows():
                try:
                    cursor.execute(insert_sql, tuple(row.values))
                    rows_inserted += 1
                except Exception as e:
                    print(f"  ⚠ Error inserting row into {table_name}: {e}")
            
            conn.commit()
            print(f"  ✓ Loaded {rows_inserted} rows into {table_name}")
        
        print("\n✓ Excel data loaded successfully!")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Error loading data: {e}")
        return False
    finally:
        conn.close()


def main():
    """Main function to run the Excel loader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Load Excel data into staging tables")
    parser.add_argument("--excel_file", default="raw_data.xlsx", help="Path to Excel file")
    parser.add_argument("--keep_existing", action="store_true", help="Keep existing data (don't truncate)")
    
    args = parser.parse_args()
    
    success = load_excel_to_staging(
        excel_file=args.excel_file,
        clear_existing=not args.keep_existing
    )
    
    if success:
        print("\n✓ Ready to run pipeline!")
        print("  Run: python main.py --start_date YYYY-MM-DD --end_date YYYY-MM-DD")
    else:
        print("\n✗ Failed to load data. Please check errors above.")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())



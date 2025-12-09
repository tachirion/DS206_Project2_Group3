import os


SQL_CFG_FILE = "sql_server_config.cfg"
SQL_CFG_SECTION = "SQL_SERVER"

DB_NAME = "ORDER_DDS"
SRC_SCHEMA = "staging"
DEST_SCHEMA = "dbo"

# Log file path (folder creation handled in logging.py)
LOG_FILE = os.path.join("logs", "logs_dimensional_data_pipeline.txt")
QUERIES_DIR = os.path.join("pipeline_dimensional_data", "queries")

queries_map = {
    "dim_categories": "update_dim_categories.sql",
    "dim_customers": "update_dim_customers.sql",
    "dim_employees": "update_dim_employees.sql",
    "dim_products": "update_dim_products.sql",
    "dim_region": "update_dim_region.sql",
    "dim_shippers": "update_dim_shippers.sql",
    "dim_suppliers": "update_dim_suppliers.sql",
    "dim_territories": "update_dim_territories.sql",
    "fact_orders": "update_fact.sql",
    "fact_error": "update_fact_error.sql",
}

dim_tables = {
    "DimCategories": f"{DEST_SCHEMA}.DimCategories",
    "DimCustomers": f"{DEST_SCHEMA}.DimCustomers",
    "DimEmployees": f"{DEST_SCHEMA}.DimEmployees",
    "DimProducts": f"{DEST_SCHEMA}.DimProducts",
    "DimRegion": f"{DEST_SCHEMA}.DimRegion",
    "DimShippers": f"{DEST_SCHEMA}.DimShippers",
    "DimSuppliers": f"{DEST_SCHEMA}.DimSuppliers",
    "DimTerritories": f"{DEST_SCHEMA}.DimTerritories",
}

FACT_TABLE = f"{DEST_SCHEMA}.FactOrders"
FACT_ERROR_TABLE = f"{DEST_SCHEMA}.fact_error"

# Order of dimensional table processing (must match task function names)
# Dimensions are processed before facts to ensure referential integrity
DIM_ORDER = [
    "dim_categories",
    "dim_customers",
    "dim_employees",
    "dim_products",
    "dim_region",
    "dim_shippers",
    "dim_suppliers",
    "dim_territories",
]

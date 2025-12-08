import os
import pyodbc
import uuid
from datetime import datetime
from typing import Dict, Any

from pipeline_dimensional_data import config
from utils import get_sql_config, create_connection_string, load_query


def _ensure_dir(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _open_connection():
    cfg = get_sql_config(config.SQL_CFG_FILE, config.SQL_CFG_SECTION)
    conn_str = create_connection_string(cfg)
    conn = pyodbc.connect(conn_str, autocommit=False)
    return conn


def _prepare_sql(sql_text: str, extra_tokens: Dict[str, str]) -> str:
    tokens = dict(config.TOKEN_MAP)
    tokens.update(extra_tokens or {})
    for k, v in tokens.items():
        placeholder = "{{" + k + "}}"
        if placeholder in sql_text:
            sql_text = sql_text.replace(placeholder, str(v))
    return sql_text


def run_sql_script(script_name: str, params: Dict[str, Any], execution_id: str) -> Dict[str, Any]:
    sql_path = os.path.join(config.QUERIES_DIR, script_name)
    if not os.path.exists(sql_path):
        return {"success": False, "error": f"SQL script not found: {sql_path}"}

    raw_sql = load_query(script_name, config.QUERIES_DIR)
    if raw_sql is None:
        with open(sql_path, "r", encoding="utf-8") as f:
            raw_sql = f.read()

    extra_tokens = {
        "START_DATE": params.get("START_DATE", ""),
        "END_DATE": params.get("END_DATE", ""),
        "EXECUTION_ID": execution_id,
        "SRC_TABLE": params.get("SRC_TABLE", ""),
        "DEST_TABLE": params.get("DEST_TABLE", ""),
    }
    sql_to_run = _prepare_sql(raw_sql, extra_tokens)

    conn = None
    try:
        conn = _open_connection()
        cursor = conn.cursor()
        batches = []
        current = []
        for line in sql_to_run.splitlines():
            if line.strip().upper() == "GO":
                if current:
                    batches.append("\n".join(current))
                    current = []
            else:
                current.append(line)
        if current:
            batches.append("\n".join(current))

        for batch in batches:
            if not batch.strip():
                continue
            cursor.execute(batch)
        conn.commit()
        return {"success": True}
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()


def task_dim_categories(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Categories",
        "DEST_TABLE": config.dim_tables["DimCategories"],
    }
    return run_sql_script(config.queries_map["dim_categories"], params, execution_id)


def task_dim_customers(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Customers",
        "DEST_TABLE": config.dim_tables["DimCustomers"],
    }
    return run_sql_script(config.queries_map["dim_customers"], params, execution_id)


def task_dim_employees(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Employees",
        "DEST_TABLE": config.dim_tables["DimEmployees"],
    }
    return run_sql_script(config.queries_map["dim_employees"], params, execution_id)


def task_dim_products(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Products",
        "DEST_TABLE": config.dim_tables["DimProducts"],
    }
    return run_sql_script(config.queries_map["dim_products"], params, execution_id)


def task_dim_region(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Region",
        "DEST_TABLE": config.dim_tables["DimRegion"],
    }
    return run_sql_script(config.queries_map["dim_region"], params, execution_id)


def task_dim_shippers(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Shippers",
        "DEST_TABLE": config.dim_tables["DimShippers"],
    }
    return run_sql_script(config.queries_map["dim_shippers"], params, execution_id)


def task_dim_suppliers(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Suppliers",
        "DEST_TABLE": config.dim_tables["DimSuppliers"],
    }
    return run_sql_script(config.queries_map["dim_suppliers"], params, execution_id)


def task_dim_territories(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Territories",
        "DEST_TABLE": config.dim_tables["DimTerritories"],
    }
    return run_sql_script(config.queries_map["dim_territories"], params, execution_id)


def task_fact_orders(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Orders",
        "DEST_TABLE": config.FACT_TABLE,
    }
    return run_sql_script(config.queries_map["fact_orders"], params, execution_id)


def task_fact_error(start_date: str, end_date: str, execution_id: str):
    params = {
        "START_DATE": start_date,
        "END_DATE": end_date,
        "SRC_TABLE": f"{config.SRC_SCHEMA}.Orders",
        "DEST_TABLE": config.FACT_ERROR_TABLE,
    }
    return run_sql_script(config.queries_map["fact_error"], params, execution_id)

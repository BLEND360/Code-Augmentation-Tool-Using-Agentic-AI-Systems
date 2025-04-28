import pandas as pd
import re
import numpy as np
import time
from databricks import sql
import snowflake.connector

def run_query(conn, query_string):
    cur = conn.cursor()
    cur.execute(query_string)
    result = cur.fetchall()
    columns = [desc[0].lower().strip() for desc in cur.description]
    return pd.DataFrame(result, columns=columns)

def run_query_with_timer(conn, query_string):
    """ Run a query and measure execution time and rows processed. """
    cur = conn.cursor()
    start_time = time.time()
    cur.execute(query_string)
    result = cur.fetchall()
    end_time = time.time()
    columns = [desc[0].lower().strip() for desc in cur.description]
    df = pd.DataFrame(result, columns=columns)
    return df, {
        "execution_time_ms": round((end_time - start_time) * 1000, 2),
        "rows_processed": len(df)
    }

def detect_sql_clauses(query):
    query_upper = query.upper()
    return {
        "has_group_by": "GROUP BY" in query_upper,
        "has_order_by": "ORDER BY" in query_upper,
        "has_having": "HAVING" in query_upper,
        "has_distinct": "DISTINCT" in query_upper,
        "has_join": "JOIN" in query_upper,
        "has_qualify": "QUALIFY" in query_upper,
        "has_limit": "LIMIT" in query_upper or "FETCH FIRST" in query_upper,
        "has_union_ops": any(op in query_upper for op in ["UNION", "INTERSECT", "EXCEPT"]),
        "has_subquery": " IN (" in query_upper or " EXISTS (" in query_upper,
        "has_case": "CASE WHEN" in query_upper or "IF(" in query_upper,
        "has_window": any(wf in query_upper for wf in ["RANK()", "ROW_NUMBER()", "DENSE_RANK()"]),
        "has_null_check": " IS NULL" in query_upper or " IS NOT NULL" in query_upper,
        "has_where": "WHERE" in query_upper
    }

def get_rounded_columns(query: str) -> dict:
    round_matches = re.findall(
        r'ROUND\s*\(.*?,\s*(\d+)\s*\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        query,
        flags=re.IGNORECASE
    )
    detected = {alias.lower(): int(precision) for precision, alias in round_matches}

    aggregate_matches = re.findall(
        r'(SUM|AVG|TOTAL|MEDIAN)\s*\(.*?\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        query,
        flags=re.IGNORECASE
    )
    for func, alias in aggregate_matches:
        alias_lower = alias.lower()
        if alias_lower not in detected:
            detected[alias_lower] = 2  # Default rounding to 2 decimals

    return detected

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [col.lower().strip() for col in df.columns]
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip().str.lower()
        else:
            df[col] = df[col].astype(str).str.strip().str.lower()
    return df

def compare_with_tolerance(df1, df2, rounded_columns: dict):
    if df1.shape != df2.shape:
        print("[X] Shape mismatch:", df1.shape, df2.shape)
        return False

    for col in df1.columns:
        try:
            col1 = df1[col]
            col2 = df2[col]

            try:
                num1 = pd.to_numeric(col1, errors='raise')
                num2 = pd.to_numeric(col2, errors='raise')

                if col in rounded_columns:
                    precision = rounded_columns[col]
                    num1 = num1.round(precision)
                    num2 = num2.round(precision)

                    str1 = num1.apply(lambda x: f"{x:.{precision}f}" if pd.notnull(x) else "")
                    str2 = num2.apply(lambda x: f"{x:.{precision}f}" if pd.notnull(x) else "")
                else:
                    str1 = num1.astype(str)
                    str2 = num2.astype(str)

                if not str1.equals(str2):
                    print(f"[X] Mismatch in numeric column '{col}':")
                    for i in range(len(str1)):
                        if str1[i] != str2[i]:
                            print(f"{i:>3} | {str1[i]} | {str2[i]}")
                    return False

            except Exception:
                str1 = col1.astype(str).str.strip().str.lower()
                str2 = col2.astype(str).str.strip().str.lower()
                if not str1.equals(str2):
                    print(f"[X] Mismatch in string column '{col}':")
                    for i in range(len(str1)):
                        if str1[i] != str2[i]:
                            print(f"{i:>3} | {str1[i]} | {str2[i]}")
                    return False

        except Exception as e:
            print(f"[X] Exception in column '{col}': {e}")
            return False

    return True

def qualify_tables(query: str, db_name: str):
    """
    Fully qualify tables in FROM and JOIN clauses with the database name.
    Handles simple cases with optional aliases.
    """
    # Detect CTEs so we don't qualify them
    cte_pattern = r"WITH\s+(.+?)\s+AS\s*\("
    cte_matches = re.findall(cte_pattern, query, flags=re.IGNORECASE | re.DOTALL)
    cte_names = set()
    if cte_matches:
        for match in cte_matches:
            parts = match.split(',')
            for part in parts:
                name = part.strip().split()[0]
                if name:
                    cte_names.add(name.lower())

    # This pattern captures FROM or JOIN followed by table name, optional alias
    table_pattern = r'\b(FROM|JOIN)\s+([a-zA-Z_][\w]*)(\s+[a-zA-Z_][\w]*)?'

    def replacer(match):
        keyword = match.group(1)
        table = match.group(2)
        alias = match.group(3) or ''

        # Skip CTEs
        if table.lower() in cte_names:
            return match.group(0)

        # If table already has a dot (.), assume already qualified
        if '.' in table:
            return match.group(0)

        return f"{keyword} {db_name}.{table.lower()}{alias}"

    return re.sub(table_pattern, replacer, query, flags=re.IGNORECASE)



def strip_sql_hints(query: str) -> str:
    """
    Remove SQL hints that are incompatible between different SQL engines
    Handles Snowflake-style hints like /*+ BROADCAST */ and similar
    """
    # Remove /*+ ... */ style hints
    hint_pattern = r'/\*\+.*?\*/'
    return re.sub(hint_pattern, '', query, flags=re.DOTALL)

def validate_query_across_engines(original_query: str, optimized_query: str, conn_sf, conn_db, db_name: str = "nbcu_demo") -> dict:
    try:
        print("Starting validation...")

        # 🔵 Prepare fully qualified queries
        query_db_orig = qualify_tables(original_query, db_name)
        query_db_opt = qualify_tables(optimized_query, db_name)
        
        # 🔵 Strip SQL hints for Databricks
        query_db_orig = strip_sql_hints(query_db_orig)
        query_db_opt = strip_sql_hints(query_db_opt)

        # 🔵 Metrics + Results (Original)
        _, metrics_sf_orig = run_query_with_timer(conn_sf, original_query)
        _, metrics_db_orig = run_query_with_timer(conn_db, query_db_orig)

        # 🔵 Metrics + Results (Optimized)
        df_sf_opt, metrics_sf_opt = run_query_with_timer(conn_sf, optimized_query)
        df_db_opt, metrics_db_opt = run_query_with_timer(conn_db, query_db_opt)


        # 🔵 Normalize for Validation
        clauses = detect_sql_clauses(optimized_query)
        rounded_columns = get_rounded_columns(optimized_query)
        strict_order = clauses.get("has_order_by", False)

        df_sf_norm = normalize_dataframe(df_sf_opt)
        df_db_norm = normalize_dataframe(df_db_opt)

        if strict_order:
            print("[Validation] ORDER BY detected — comparing row-by-row.")
            match = compare_with_tolerance(df_sf_norm, df_db_norm, rounded_columns)
        else:
            print("[Validation] No ORDER BY — comparing sorted DataFrames.")
            df_sf_sorted = df_sf_norm.sort_values(by=list(df_sf_norm.columns)).reset_index(drop=True)
            df_db_sorted = df_db_norm.sort_values(by=list(df_db_norm.columns)).reset_index(drop=True)
            match = compare_with_tolerance(df_sf_sorted, df_db_sorted, rounded_columns)

        # 🔵 Prepare Metrics Table
        kpi_table = pd.DataFrame({
            "KPI": ["Execution Time (ms)", "Rows Processed"],
            "Snowflake (Original)": [metrics_sf_orig["execution_time_ms"], metrics_sf_orig["rows_processed"]],
            "Snowflake (Optimized)": [metrics_sf_opt["execution_time_ms"], metrics_sf_opt["rows_processed"]],
            "Databricks (Original)": [metrics_db_orig["execution_time_ms"], metrics_db_orig["rows_processed"]],
            "Databricks (Optimized)": [metrics_db_opt["execution_time_ms"], metrics_db_opt["rows_processed"]],
        })

        return {
            "validation_status": "success" if match else "fail",
            "failed_checks": [] if match else [{
                "check": "data_match",
                "reason": "Row values differ (check row order or precision)"
            }],
            "performance_metrics": kpi_table.to_dict(orient="records")  # ready for Streamlit
        }

    except Exception as e:
        return {
            "validation_status": "error",
            "failed_checks": [{"check": "execution", "reason": str(e)}]
        }
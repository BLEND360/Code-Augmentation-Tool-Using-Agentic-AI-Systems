import pandas as pd
from databricks import sql
import snowflake.connector
import re
import numpy as np

def run_query(conn, query_string):
    cur = conn.cursor()
    cur.execute(query_string)
    result = cur.fetchall()
    columns = [desc[0].lower().strip() for desc in cur.description]
    return pd.DataFrame(result, columns=columns)

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

    df = df.sort_index(axis=1)
    df = df.sort_values(by=list(df.columns)).reset_index(drop=True)
    return df

def compare_with_tolerance(df1, df2, tolerance=1e-3):
    if df1.shape != df2.shape:
        return False

    for col in df1.columns:
        if pd.api.types.is_numeric_dtype(df1[col]) and pd.api.types.is_numeric_dtype(df2[col]):
            if not np.allclose(df1[col], df2[col], rtol=tolerance, atol=tolerance, equal_nan=True):
                return False
        else:
            if not df1[col].equals(df2[col]):
                return False

    return True


def validate_results(df_sf, df_db, clauses):
    failed_checks = []

    df_sf_norm = normalize_dataframe(df_sf)
    df_db_norm = normalize_dataframe(df_db)

    # ✅ Check column names
    if set(df_sf_norm.columns) != set(df_db_norm.columns):
        failed_checks.append({
            "check": "column_names",
            "reason": f"Snowflake={sorted(df_sf_norm.columns)}, Databricks={sorted(df_db_norm.columns)}"
        })

    # ✅ Row count check
    if df_sf_norm.shape != df_db_norm.shape:
        failed_checks.append({
            "check": "row_count",
            "reason": f"Snowflake rows: {df_sf_norm.shape[0]}, Databricks rows: {df_db_norm.shape[0]}"
        })

    # ✅ Compare row values
    ordered_match = compare_with_tolerance(df_sf_norm, df_db_norm, tolerance=1e-3)

    if ordered_match:
        pass  # ✅ All good
    else:
        # Sort if ORDER BY is not enforced
        if not clauses.get("has_order_by", False):
            df_sf_sorted = df_sf_norm.sort_values(by=list(df_sf_norm.columns)).reset_index(drop=True)
            df_db_sorted = df_db_norm.sort_values(by=list(df_db_norm.columns)).reset_index(drop=True)
            sorted_match = compare_with_tolerance(df_sf_sorted, df_db_sorted, tolerance=1e-3)

            if sorted_match:
                failed_checks.append({
                    "check": "row_order_mismatch",
                    "reason": "Row values match but order differs between Snowflake and Databricks"
                })
            else:
                failed_checks.append({
                    "check": "data_match",
                    "reason": "Row values or numeric precision differ across engines"
                })
        else:
            failed_checks.append({
                "check": "data_match",
                "reason": "Row values or numeric precision differ across engines"
            })

    # DISTINCT check
    if clauses.get("has_distinct"):
        if df_sf_norm.duplicated().any() or df_db_norm.duplicated().any():
            failed_checks.append({
                "check": "distinct",
                "reason": "Duplicate rows found despite DISTINCT"
            })

    # HAVING
    if clauses.get("has_having") and df_sf_norm.shape[0] != df_db_norm.shape[0]:
        failed_checks.append({
            "check": "having",
            "reason": "Row count mismatch after HAVING clause"
        })

    # NULLs
    if clauses.get("has_null_check"):
        if df_sf_norm.isnull().sum().sum() != df_db_norm.isnull().sum().sum():
            failed_checks.append({
                "check": "null_handling",
                "reason": "Mismatch in NULL value counts"
            })

    return {
        "validation_status": "success" if not failed_checks else "fail",
        "failed_checks": failed_checks
    }

    
def qualify_tables(query: str, db_name: str):
    """
    Prefix base tables in FROM and JOIN clauses with the database name,
    but skip CTEs and already-qualified tables.
    """
    # Step 1: Extract all CTE names from the WITH clause
    cte_pattern = r"WITH\s+(.+?)\s+AS\s*\("
    cte_matches = re.findall(cte_pattern, query, flags=re.IGNORECASE | re.DOTALL)

    cte_names = set()
    if cte_matches:
        for match in cte_matches:
            # Handles multiple CTEs separated by comma
            parts = match.split(',')
            for part in parts:
                name = part.strip().split()[0]
                if name:
                    cte_names.add(name.lower())

    # Step 2: Replace FROM/JOIN base tables (ignore already qualified or CTEs)
    table_pattern = r"\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?!\s*\.)"

    def replacer(match):
        keyword, table = match.groups()
        if table.lower() in cte_names:
            return match.group(0)  # Don't modify CTE names
        return f"{keyword} {db_name}.{table.lower()}"

    return re.sub(table_pattern, replacer, query, flags=re.IGNORECASE)



def validate_query_across_engines(query_string, conn_sf, conn_db, db_name="nbcu_demo"):
    try:
        print("Starting validation...")
        query_for_databricks = qualify_tables(query_string, db_name)

        print(f"Snowflake Query: {query_string}")
        print(f"Databricks Query: {query_for_databricks}")

        df_sf = run_query(conn_sf, query_string)
        df_db = run_query(conn_db, query_for_databricks)

        clauses = detect_sql_clauses(query_string)
        result = validate_results(df_sf, df_db, clauses)

        return result

    except Exception as e:
        return {
            "validation_status": "error",
            "failed_checks": [{"check": "execution", "reason": str(e)}]
        }

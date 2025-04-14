import pandas as pd
import re
import numpy as np
from databricks import sql
import snowflake.connector

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

def get_rounded_columns(query: str) -> dict:
    round_matches = re.findall(
        r'ROUND\s*\(.*?,\s*(\d+)\s*\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        query,
        flags=re.IGNORECASE
    )
    detected = {alias.lower(): int(precision) for precision, alias in round_matches}

    # 🔹 Add default rounding for known aggregates if ROUND not used explicitly
    aggregate_matches = re.findall(
        r'(SUM|AVG|TOTAL|MEDIAN)\s*\(.*?\)\s+AS\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        query,
        flags=re.IGNORECASE
    )
    for func, alias in aggregate_matches:
        alias_lower = alias.lower()
        if alias_lower not in detected:
            detected[alias_lower] = 2  # Default precision fallback

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
                    print("Row | Snowflake | Databricks")
                    for i in range(len(str1)):
                        if str1[i] != str2[i]:
                            print(f"{i:>3} | {str1[i]:>10} | {str2[i]:<10}")
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

    table_pattern = r"\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)(?!\s*\.)"

    def replacer(match):
        keyword, table = match.groups()
        if table.lower() in cte_names:
            return match.group(0)
        return f"{keyword} {db_name}.{table.lower()}"

    return re.sub(table_pattern, replacer, query, flags=re.IGNORECASE)

def validate_query_across_engines(query_string: str, conn_sf, conn_db, db_name: str = "nbcu_demo") -> dict:
    try:
        print("Starting validation...")
        query_db = qualify_tables(query_string, db_name)

        df_sf = run_query(conn_sf, query_string)
        df_db = run_query(conn_db, query_db)

        clauses = detect_sql_clauses(query_string)
        rounded_columns = get_rounded_columns(query_string)
        strict_order = clauses.get("has_order_by", False)

        df_sf_norm = normalize_dataframe(df_sf)
        df_db_norm = normalize_dataframe(df_db)

        if strict_order:
            print("[Validation] ORDER BY detected — comparing row-by-row.")
            match = compare_with_tolerance(df_sf_norm, df_db_norm, rounded_columns)
        else:
            print("[Validation] No ORDER BY — comparing sorted DataFrames.")
            df_sf_sorted = df_sf_norm.sort_values(by=list(df_sf_norm.columns)).reset_index(drop=True)
            df_db_sorted = df_db_norm.sort_values(by=list(df_db_norm.columns)).reset_index(drop=True)
            match = compare_with_tolerance(df_sf_sorted, df_db_sorted, rounded_columns)

        return {
            "validation_status": "success" if match else "fail",
            "failed_checks": [] if match else [{
                "check": "data_match",
                "reason": "Row values differ (check row order or precision)"
            }]
        }

    except Exception as e:
        return {
            "validation_status": "error",
            "failed_checks": [{"check": "execution", "reason": str(e)}]
        }
import pandas as pd
import re
import numpy as np
import time
from databricks import sql
import snowflake.connector
import pprint

def run_query(conn, query_string):
    cur = conn.cursor()
    cur.execute(query_string)
    result = cur.fetchall()
    columns = [desc[0].lower().strip() for desc in cur.description]
    return pd.DataFrame(result, columns=columns)

import time
import pandas as pd


def run_query_with_timer(conn, query_string):
    """Run a query and capture detailed Snowflake execution metrics, always bypassing result cache."""
    cur = conn.cursor()

    # Detect if it's a Snowflake connection
    is_snowflake = hasattr(cur, "sfqid")

    # Desactivar permanentemente el cache para la sesión
    if is_snowflake:
        try:
            cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")
            print("⚠️ Snowflake result cache disabled at session level.")
        except Exception as e:
            print(f"⚠️ Could not disable result cache: {e}")

        # Agregar hint a la query si no lo tiene
        if "/*+ NO_RESULT_CACHE */" not in query_string.upper():
            query_string = f"SELECT /*+ NO_RESULT_CACHE */ " + query_string.lstrip().lstrip("SELECT ").lstrip()

    start_time = time.time()

    try:
        cur.execute(query_string)
        result = cur.fetchall()
        end_time = time.time()

        columns = [desc[0].lower().strip() for desc in cur.description]
        df = pd.DataFrame(result, columns=columns)

        wall_clock_execution_time_ms = round((end_time - start_time) * 1000, 2)
        execution_time_ms = wall_clock_execution_time_ms
        rows_processed = len(df)

        if is_snowflake:
            query_id = cur.sfqid
            print(f"[SNOWFLAKE] Query ID: {query_id}")

            metadata_cursor = conn.cursor()
            metadata_cursor.execute(f"""
                SELECT
                    execution_time,
                    rows_produced
                FROM table(information_schema.query_history_by_session())
                WHERE query_id = '{query_id}'
            """)
            row = metadata_cursor.fetchone()
            metadata_cursor.close()

            if row:
                (
                    execution_time,
                    rows_produced
                ) = row

                execution_time_ms = execution_time

                delta = round(wall_clock_execution_time_ms - execution_time, 2)
                print(f"[SNOWFLAKE] Execution time (ms): {execution_time} Vs wall clock time (ms): {wall_clock_execution_time_ms}")
                print(f"[DELTA] Wall clock - engine time (ms): {delta}")
                print(f"[SNOWFLAKE] Rows produced: {rows_produced} Vs Python length: {len(df)}")
            else:
                print(f"[SNOWFLAKE] Query ID {query_id} not found in query history. Using wall clock time.")

        return df, {
            "execution_time_ms": execution_time_ms,
            "rows_processed": rows_processed
        }

    except Exception as e:
        end_time = time.time()
        error_message = str(e)
        print(f"[ERROR] Query execution failed: {error_message}")
        return pd.DataFrame(), {
            "execution_time_ms": round((end_time - start_time) * 1000, 2),
            "rows_processed": 0,
            "error": error_message
        }

    finally:
        cur.close()

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


def strip_sql_hints(query: str) -> str:
    """
    Remove SQL hints that are incompatible between different SQL engines
    Handles Snowflake-style hints like /*+ BROADCAST */ and similar
    """
    # Remove /*+ ... */ style hints
    hint_pattern = r'/\*\+.*?\*/'
    return re.sub(hint_pattern, '', query, flags=re.DOTALL)

def qualify_tables(query: str, db_name: str):
    """
    Fully qualify tables in FROM and JOIN clauses with the database name.
    Checks if tables are already qualified to avoid double qualification.
    """
    # Detect CTEs so we don't qualify them
    cte_pattern = r"WITH\s+(.+?)\s+AS\s*\("
    cte_matches = re.findall(cte_pattern, query, flags=re.IGNORECASE | re.DOTALL)
    cte_names = set()
    if cte_matches:
        for match in cte_matches:
            # Handle multiple CTEs separated by commas
            cte_sections = re.split(r',\s*(?=[a-zA-Z_][\w]*\s+AS\s*\()', match)
            for section in cte_sections:
                # Extract CTE name
                cte_name_match = re.match(r'([a-zA-Z_][\w]*)', section.strip())
                if cte_name_match:
                    cte_names.add(cte_name_match.group(1).lower())

    # This pattern captures FROM or JOIN followed by table name, optional alias
    # More complex to handle table names with or without aliases
    table_pattern = r'\b(FROM|JOIN)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)((?:\s+(?:AS\s+)?[a-zA-Z_][\w]*)?)'

    def replacer(match):
        keyword = match.group(1)  # FROM or JOIN
        table = match.group(2)    # Table name (possibly with qualifier)
        alias_part = match.group(3) or ''  # Alias part (including potential AS)

        # Skip CTEs
        if table.lower() in cte_names:
            return match.group(0)

        # Skip if already qualified with the same database name
        if table.lower().startswith(f"{db_name.lower()}."):
            return match.group(0)
            
        # Skip if has any qualifier (contains a dot)
        if '.' in table:
            return match.group(0)

        return f"{keyword} {db_name}.{table}{alias_part}"

    # Apply the regex replacement
    qualified_query = re.sub(table_pattern, replacer, query, flags=re.IGNORECASE)
    
    print(f"Original query: {query}")
    print(f"Qualified query: {qualified_query}")
    
    return qualified_query

def retry_databricks_query(conn, query_string, db_name, max_retries=1):
    """
    Run a query on Databricks with automatic retry logic for table not found errors.
    Only measures execution time for the successful attempt.
    
    Args:
        conn: Databricks connection object
        query_string: SQL query to execute
        db_name: Database name for table qualification
        max_retries: Maximum number of retries (default=1)
        
    Returns:
        Tuple of (result DataFrame, metrics dictionary)
    """
    # First try - use the query as-is
    df, metrics = run_query_with_timer(conn, query_string)
    
    # Check if we got an error about table not found
    retry_count = 0
    while "error" in metrics and "TABLE_OR_VIEW_NOT_FOUND" in metrics["error"] and retry_count < max_retries:
        retry_count += 1
        print(f"Table not found error, retrying (attempt {retry_count}/{max_retries})...")
        
        # For the retry, explicitly qualify all tables with the database name
        # But don't qualify if already qualified
        qualified_query = qualify_tables(query_string, db_name)
        
        # Wait a moment to allow metadata to be loaded (helps with connection issues)
        time.sleep(1)
        
        # Try again with the qualified query - this is the timing that matters
        df, metrics = run_query_with_timer(conn, qualified_query)
    
    # Add a flag to indicate if we used a retry
    if retry_count > 0 and "error" not in metrics:
        metrics["used_retry"] = True
    
    return df, metrics

def warm_up_databricks_connection(conn, db_name):
    """
    Run a simple query to warm up the Databricks connection and cache table metadata.
    This helps ensure more accurate timing for subsequent queries.
    """
    try:
        # Simple query to list tables in the database
        warm_up_query = f"SHOW TABLES IN {db_name}"
        conn.cursor().execute(warm_up_query)
        print(f"Connection to {db_name} warmed up successfully")
        
        # Wait a moment for metadata to be loaded
        time.sleep(1)
    except Exception as e:
        print(f"Warning: Failed to warm up connection: {e}")

def validate_query_across_engines(original_query: str, optimized_query: str, conn_sf, conn_db, db_name: str = "nbcu_demo") -> dict:
    try:
        print("Starting validation...")
        
        # Warm up the Databricks connection first to load metadata
        try:
            warm_up_query = f"SHOW TABLES IN {db_name}"
            conn_db.cursor().execute(warm_up_query)
            print(f"Connection to {db_name} warmed up successfully")
            time.sleep(1)
        except Exception as e:
            print(f"Warning: Failed to warm up connection: {e}")

        # 🔵 Strip SQL hints for Databricks
        db_orig_query = strip_sql_hints(original_query)
        db_opt_query = strip_sql_hints(optimized_query)
        
        # Ensure both queries have the same level of table qualification
        db_orig_query = qualify_tables(db_orig_query, db_name)
        db_opt_query = qualify_tables(db_opt_query, db_name)

        # 🔵 Metrics + Results (Original)
        df_sf_orig, metrics_sf_orig = run_query_with_timer(conn_sf, original_query)
        print("-------")
        print(metrics_sf_orig)
        # Check for errors in Snowflake query
        if "error" in metrics_sf_orig:
            return {
                "validation_status": "error",
                "failed_checks": [{"check": "execution", "reason": f"Snowflake original query error: {metrics_sf_orig['error']}"}]
            }
        
        # Use retry logic for Databricks to handle table not found errors
        df_db_orig, metrics_db_orig = run_query_with_timer(conn_db, db_orig_query)
        
        # Retry if table not found error
        retry_count_orig = 0
        while "error" in metrics_db_orig and "TABLE_OR_VIEW_NOT_FOUND" in metrics_db_orig["error"] and retry_count_orig < 1:
            retry_count_orig += 1
            print(f"Table not found in original query, retrying...")
            time.sleep(1)
            df_db_orig, metrics_db_orig = run_query_with_timer(conn_db, db_orig_query)
        
        # Check for errors in Databricks query after retries
        if "error" in metrics_db_orig:
            return {
                "validation_status": "error",
                "failed_checks": [{"check": "execution", "reason": f"Databricks original query error: {metrics_db_orig['error']}"}]
            }

        # 🔵 Metrics + Results (Optimized)
        df_sf_opt, metrics_sf_opt = run_query_with_timer(conn_sf, optimized_query)
        print("-------")
        print(metrics_sf_orig)
        # Check for errors in optimized Snowflake query
        if "error" in metrics_sf_opt:
            return {
                "validation_status": "error",
                "failed_checks": [{"check": "execution", "reason": f"Snowflake optimized query error: {metrics_sf_opt['error']}"}]
            }
        
        # Use retry logic for optimized Databricks query
        df_db_opt, metrics_db_opt = run_query_with_timer(conn_db, db_opt_query)
        
        # Retry if table not found error
        retry_count_opt = 0
        while "error" in metrics_db_opt and "TABLE_OR_VIEW_NOT_FOUND" in metrics_db_opt["error"] and retry_count_opt < 1:
            retry_count_opt += 1
            print(f"Table not found in optimized query, retrying...")
            time.sleep(1)
            df_db_opt, metrics_db_opt = run_query_with_timer(conn_db, db_opt_query)
        
        # Check for errors in optimized Databricks query after retries
        if "error" in metrics_db_opt:
            return {
                "validation_status": "error",
                "failed_checks": [{"check": "execution", "reason": f"Databricks optimized query error: {metrics_db_opt['error']}"}]
            }

        # 🔵 Clean the cache between queries to ensure fair comparison
        try:
            conn_db.cursor().execute("CLEAR CACHE")
            print("Databricks cache cleared between queries")
        except Exception as e:
            print(f"Warning: Failed to clear Databricks cache: {e}")

        # 🔵 Run the optimized query again for accurate timing
        if retry_count_opt == 0:  # Only if we didn't already retry
            df_db_opt_rerun, metrics_db_opt_rerun = run_query_with_timer(conn_db, db_opt_query)
            
            # Use the better timing between the two runs
            if "error" not in metrics_db_opt_rerun:
                metrics_db_opt = metrics_db_opt_rerun

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

        # Track if we used retries
        used_retry_orig = retry_count_orig > 0
        used_retry_opt = retry_count_opt > 0

        # 🔵 Prepare Metrics Table with numeric values only
        kpi_table = pd.DataFrame({
            "KPI": ["Execution Time (ms)", "Rows Processed"],
            "Snowflake (Original)": [metrics_sf_orig["execution_time_ms"], metrics_sf_orig["rows_processed"]],
            "Snowflake (Optimized)": [metrics_sf_opt["execution_time_ms"], metrics_sf_opt["rows_processed"]],
            "Databricks (Original)": [metrics_db_orig["execution_time_ms"], metrics_db_orig["rows_processed"]],
            "Databricks (Optimized)": [metrics_db_opt["execution_time_ms"], metrics_db_opt["rows_processed"]],
        })

        print(kpi_table)

        # Add retry notes as separate information
        retry_notes = []
        if used_retry_orig:
            retry_notes.append("The original Databricks query required a retry.")
        if used_retry_opt:
            retry_notes.append("The optimized Databricks query required a retry.")

        # Include retry notes in the result
        result = {
            "validation_status": "success" if match else "fail",
            "failed_checks": [] if match else [{
                "check": "data_match",
                "reason": "Row values differ (check row order or precision)"
            }],
            "performance_metrics": kpi_table.to_dict(orient="records"),  # ready for Streamlit
            "retry_notes": retry_notes if retry_notes else []
        }

        return result

    except Exception as e:
        return {
            "validation_status": "error",
            "failed_checks": [{"check": "execution", "reason": str(e)}]
        }
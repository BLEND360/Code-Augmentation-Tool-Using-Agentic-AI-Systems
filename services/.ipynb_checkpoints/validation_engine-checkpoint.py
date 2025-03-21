# validation_engine.py

import pandas as pd

def run_query(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
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

def validate_results(df_sf, df_db, clauses):
    failed_checks = []

    if df_sf.shape != df_db.shape:
        failed_checks.append({"check": "row_count", "reason": f"Snowflake={df_sf.shape}, Databricks={df_db.shape}"})

    if set(df_sf.columns) != set(df_db.columns):
        failed_checks.append({"check": "column_names", "reason": "Column sets differ"})

    if clauses["has_order_by"]:
        if not df_sf.equals(df_db):
            failed_checks.append({"check": "order_by", "reason": "Row order mismatch"})

    if clauses["has_group_by"]:
        if not df_sf.equals(df_db):
            failed_checks.append({"check": "group_by", "reason": "Aggregated/grouped results differ"})

    if clauses["has_having"]:
        if df_sf.shape[0] != df_db.shape[0]:
            failed_checks.append({"check": "having", "reason": "Row count mismatch after HAVING clause"})

    if clauses["has_distinct"]:
        if df_sf.duplicated().any() or df_db.duplicated().any():
            failed_checks.append({"check": "distinct", "reason": "Duplicate rows found despite DISTINCT"})

    if clauses["has_join"]:
        if df_sf.shape[0] != df_db.shape[0]:
            failed_checks.append({"check": "join", "reason": "Join row counts differ"})

    if clauses["has_null_check"]:
        if df_sf.isnull().sum().sum() != df_db.isnull().sum().sum():
            failed_checks.append({"check": "null_handling", "reason": "NULL counts differ across columns"})

    if not failed_checks:
        return {"validation_status": "success", "failed_checks": []}
    else:
        return {"validation_status": "fail", "failed_checks": failed_checks}

def validate_query_across_engines(sql, conn_sf, conn_db):
    try:
        df_sf = run_query(conn_sf, sql)
        df_db = run_query(conn_db, sql)
        clauses = detect_sql_clauses(sql)
        result = validate_results(df_sf, df_db, clauses)
        return result
    except Exception as e:
        return {"validation_status": "error", "failed_checks": [{"check": "execution", "reason": str(e)}]}
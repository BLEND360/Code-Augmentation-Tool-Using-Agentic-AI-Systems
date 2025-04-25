import json
import os
import streamlit as st
import yaml
from typing import TypedDict, Annotated, Union
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from utils import ConverterState
from databricks import sql  
import snowflake.connector
from services.validation_engine import validate_query_across_engines
from services.db_connectors import connect_to_snowflake, connect_to_databricks
from services.query_processor import parse_sql_to_ast, translate_ast_to_ansi, validate_ansi_sql, optimize_joins_aggregations, optimize_simplify_query, optimize_data_filtering, coordinate_results
import time

# 🔹 Load YAML config
with open("services/config_file.yaml", "r") as f:
    config = yaml.safe_load(f)

def convert_snowflake_to_ansi(sql_query: str):
    st.write("✅ Entered convert_snowflake_to_ansi")

    intermediate_results = {}

    initial_state = ConverterState(
        input_query=sql_query,
        ast=None,
        translated_sql="",
        join_agg_optimized_sql="",
        simplified_sql="",
        filtered_sql="",
        final_optimized_sql="",
        optimization_notes="",
        messages=[] 
    )

    st.write("✅ About to run LangGraph pipeline")
    final_state = app.invoke(initial_state)
    optimized_sql = final_state.get("final_optimized_sql", "")

    if optimized_sql:
        with st.spinner("🔄 Connecting to Snowflake..."):
            conn_sf = connect_to_snowflake(config["snowflake"])
        with st.spinner("🔄 Connecting to Databricks..."):
            conn_db = connect_to_databricks(config["databricks"])
    
        with st.spinner("🔍 Running validation across Snowflake and Databricks..."):
            validation_result = validate_query_across_engines(
                optimized_sql,
                conn_sf,
                conn_db,
                db_name=config["databricks"].get("database", "nbcu_demo")
            )
        intermediate_results["validation_result"] = validation_result

        conn_sf.close()
        conn_db.close()

    # Always store AST and other intermediate outputs
    intermediate_results["AST"] = final_state.get("ast", {})
    intermediate_results["translated_ansi_sql"] = final_state.get("translated_sql", "")
    intermediate_results["join_agg_optimized_sql"] = final_state.get("join_agg_optimized_sql", "")
    intermediate_results["simplified_sql"] = final_state.get("simplified_sql", "")
    intermediate_results["filtered_sql"] = final_state.get("filtered_sql", "")
    intermediate_results["optimization_notes"]=final_state.get("optimization_notes","")

    return optimized_sql, intermediate_results


# 🧠 Define LangGraph workflow
workflow = StateGraph(ConverterState)
workflow.add_node("ParserAgent", parse_sql_to_ast)
workflow.add_node("TranslationAgent", translate_ast_to_ansi)
workflow.add_node("SyntaxValidatorAgent", validate_ansi_sql)
workflow.add_node("JoinAggregationOptimizerAgent", optimize_joins_aggregations)
workflow.add_node("QuerySimplificationAgent", optimize_simplify_query)
workflow.add_node("DataFilteringAgent", optimize_data_filtering)
workflow.add_node("CoordinatorAgent", coordinate_results)

workflow.set_entry_point("ParserAgent")

workflow.add_edge("ParserAgent", "TranslationAgent")
workflow.add_edge("TranslationAgent", "SyntaxValidatorAgent")
workflow.add_edge("SyntaxValidatorAgent", "JoinAggregationOptimizerAgent")
workflow.add_edge("SyntaxValidatorAgent", "QuerySimplificationAgent")
workflow.add_edge("SyntaxValidatorAgent", "DataFilteringAgent")

# Connect to coordinator
workflow.add_edge("JoinAggregationOptimizerAgent", "CoordinatorAgent")
workflow.add_edge("QuerySimplificationAgent", "CoordinatorAgent")
workflow.add_edge("DataFilteringAgent", "CoordinatorAgent")

# Final output
workflow.add_edge("CoordinatorAgent", END)

app = workflow.compile()


# 🌐 Streamlit UI
st.set_page_config(page_title="Code Augmentation Using Agentic AI", layout="wide")
st.markdown(
    """
    <style>
    .stChatMessage {
        font-size: 14px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Code Augmentation Using Agentic AI")

# Initialize session state for chat history
if "interactive_chat_history" not in st.session_state:
    st.session_state.interactive_chat_history = []

# Display previous messages
if st.session_state.interactive_chat_history:
    for chat in st.session_state.interactive_chat_history:
        with st.chat_message("user"):
            st.text(chat["question"])  # Display user's input

        with st.chat_message("assistant"):
            if isinstance(chat["answer"], str) and "Error" in chat["answer"]:
                st.error(chat["answer"])  # Display error if it exists
            else:
                st.code(chat["answer"], language="sql")  # Display the main result

            # Show intermediate results in an expander
            if "intermediate" in chat and chat["intermediate"]:
                timestamp_key = chat.get("timestamp_key", str(time.time()))
                with st.expander("View Intermediate Steps", expanded=False):  # Default not expanded
                    intermediate = chat["intermediate"]
                    
                    if "AST" in intermediate and st.checkbox("AST Tree", key=f"AST_{timestamp_key}"):
                        st.json(intermediate["AST"])
                    if "translated_ansi_sql" in intermediate and st.checkbox("Translated ANSI SQL", key=f"translated_ansi_sql_{timestamp_key}"):
                        st.code(intermediate["translated_ansi_sql"], language="sql")
                    if "join_agg_optimized_sql" in intermediate and st.checkbox("Joins & Aggregations Optimized ANSI SQL", key=f"join_agg_optimized_sql_{timestamp_key}"):
                        st.code(intermediate["join_agg_optimized_sql"], language="sql")
                    if "simplified_sql" in intermediate and st.checkbox("Simplified ANSI SQL", key=f"simplified_sql_{timestamp_key}"):
                        st.code(intermediate["simplified_sql"], language="sql")
                    if "filtered_sql" in intermediate and st.checkbox("Filtered ANSI SQL", key=f"filtered_sql_{timestamp_key}"):
                        st.code(intermediate["filtered_sql"], language="sql")
                    if "optimization_notes" in intermediate and st.checkbox("Final Optimized ANSI SQL Explanation", key=f"optimization_notes_{timestamp_key}"):
                        st.write(intermediate["optimization_notes"])
                    if "validation_result" in intermediate and st.checkbox("Validation Against Both Databases", key=f"optimization_notes_{timestamp_key}"):
                        validation_engine_result = intermediate["validation_result"]
                        if validation_engine_result.get("validation_status") == "success":
                            st.success("✅ Query output matched in both Snowflake and Databricks.")
                        else:
                            st.error("❌ Validation failed!")
                            for issue in validation_engine_result.get("failed_checks", []):
                                st.markdown(f"- **{issue['check']}**: {issue['reason']}")

# Chat input
user_question = st.chat_input("Type your Snowflake SQL query...")
if user_question:
    timestamp_key = str(time.time())
    with st.chat_message("user"):
        st.text(user_question)

    try:
        ansi_result, intermediate_results = convert_snowflake_to_ansi(user_question)
        success = True
    except Exception as e:
        success = False
        intermediate_results = {}
        ansi_result = f"Error: {e}"

    st.session_state.interactive_chat_history.append({
        "question": user_question,
        "answer": ansi_result,
        "intermediate": intermediate_results,
        "timestamp_key": timestamp_key
    })

    with st.chat_message("assistant"):
        if success:
            st.code(ansi_result, language="sql")  # Display ANSI SQL as a code block
        else:
            st.error(ansi_result)  # Display error message

        if success and intermediate_results:
            with st.expander("View Intermediate Steps"):
                if "AST" in intermediate_results and st.checkbox("AST Tree", key=f"AST_{timestamp_key}"):
                    st.json(intermediate_results["AST"])
                if "translated_ansi_sql" in intermediate_results and st.checkbox("Translated ANSI SQL", key=f"translated_ansi_sql_{timestamp_key}"):
                    st.code(intermediate_results["translated_ansi_sql"], language="sql")
                if "join_agg_optimized_sql" in intermediate_results and st.checkbox("Joins & Aggregations Optimized ANSI SQL",key=f"join_agg_optimized_sql_{timestamp_key}"):
                    st.code(intermediate_results["join_agg_optimized_sql"], language="sql")
                if "simplified_sql" in intermediate_results and st.checkbox("Simplified ANSI SQL", key=f"simplified_sql_{timestamp_key}"):
                    st.code(intermediate_results["simplified_sql"], language="sql")
                if "filtered_sql" in intermediate_results and st.checkbox("Filtered ANSI SQL", key=f"filtered_sql_{timestamp_key}"):
                    st.code(intermediate_results["filtered_sql"], language="sql")
                if "optimization_notes" in intermediate_results and st.checkbox("Final Optimized ANSI SQL Explanation", key=f"optimization_notes_{timestamp_key}"):
                    st.write(intermediate_results["optimization_notes"])
                if "validation_result" in intermediate_results and st.checkbox("Validation Against Both Databases", key=f"optimization_notes_{timestamp_key}"):
                    validation_engine_result = intermediate["validation_result"]
                    if validation_engine_result.get("validation_status") == "success":
                        st.success("✅ Query output matched in both Snowflake and Databricks.")
                    else:
                        st.error("❌ Validation failed!")
                        for issue in validation_engine_result.get("failed_checks", []):
                            st.markdown(f"- **{issue['check']}**: {issue['reason']}")

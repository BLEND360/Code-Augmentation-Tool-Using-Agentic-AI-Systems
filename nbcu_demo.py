import json
import os
import streamlit as st
import yaml
import pandas as pd
from typing import TypedDict, Annotated, Union
from langgraph.graph import StateGraph
from langgraph.graph import END
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph.message import add_messages
from utils import ConverterState
from databricks import sql  
from snowflake import connector
from services.validation_engine import validate_query_across_engines#, validate_query_across_engines2
from services.db_connectors import connect_to_snowflake, connect_to_databricks
from services.query_processor import parse_sql_to_ast, translate_ast_to_ansi, validate_ansi_sql, optimize_joins_aggregations, optimize_simplify_query, optimize_data_filtering, coordinate_results, document_final_sql
import time

# 🔹 Load YAML config
with open("services/config_file.yaml", "r") as f:
    config = yaml.safe_load(f)

def convert_snowflake_to_ansi(sql_query: str):
    with st.spinner("Converting source to destination platform code"):

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
            final_sql_documentation="",
            messages=[] 
        )

    time.sleep(0.5)

    with st.spinner("Initiating code optimization agentic AI system"):
        final_state = app.invoke(initial_state)
    
    original_query = sql_query
    optimized_sql = final_state.get("final_optimized_sql", "")

    if optimized_sql:
        with st.spinner("🔄 Connecting to Snowflake..."):
            conn_sf = connect_to_snowflake(config["snowflake"])
        with st.spinner("🔄 Connecting to Databricks..."):
            conn_db = connect_to_databricks(config["databricks"])
    
        with st.spinner("🔍 Running validation across Snowflake and Databricks..."):
            validation_result = validate_query_across_engines(
                original_query=sql_query,
                optimized_query=optimized_sql,
                conn_sf=conn_sf,
                conn_db=conn_db,
                db_name=config["databricks"].get("database", "nbcu_demo")
            )
    intermediate_results["validation_result"] = validation_result
    intermediate_results["performance_metrics"] = validation_result.get("performance_metrics", [])

    conn_sf.close()
    conn_db.close()


    # Always store AST and other intermediate outputs
    intermediate_results["AST"] = final_state.get("ast", {})
    intermediate_results["translated_ansi_sql"] = final_state.get("translated_sql", "")
    intermediate_results["join_agg_optimized_sql"] = final_state.get("join_agg_optimized_sql", "")
    intermediate_results["simplified_sql"] = final_state.get("simplified_sql", "")
    intermediate_results["filtered_sql"] = final_state.get("filtered_sql", "")
    intermediate_results["optimization_notes"] = final_state.get("optimization_notes", "")
    intermediate_results["final_sql_documentation"] = final_state.get("final_sql_documentation", "")

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
workflow.add_node("DocumentationAgent", document_final_sql)

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
workflow.add_edge("CoordinatorAgent", "DocumentationAgent")
workflow.add_edge("DocumentationAgent", END)

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
                with st.expander("View All Results", expanded=False):
                    intermediate = chat["intermediate"]
                    
                    if "AST" in intermediate and st.checkbox("**1.** Intermediary Code Logic Tree", key=f"AST_{timestamp_key}"):
                        st.json(intermediate["AST"])
            
                    if "translated_ansi_sql" in intermediate and st.checkbox("**2.** Translated destination platform code (ANSI SQL)", key=f"translated_sql_{timestamp_key}"):
                        st.code(intermediate["translated_ansi_sql"], language="sql")
            
                    if "join_agg_optimized_sql" in intermediate and st.checkbox("**3a.** Join SQL Code Optimization AI Agent Output", key=f"join_agg_sql_{timestamp_key}"):
                        st.code(intermediate["join_agg_optimized_sql"], language="sql")
            
                    if "simplified_sql" in intermediate and st.checkbox("**3b.** Simplification AI Agent Output", key=f"simplified_sql_{timestamp_key}"):
                        st.code(intermediate["simplified_sql"], language="sql")
            
                    if "filtered_sql" in intermediate and st.checkbox("**3c.** Efficient Filtering AI Agent Output", key=f"filtered_sql_{timestamp_key}"):
                        st.code(intermediate["filtered_sql"], language="sql")
            
                    if "optimization_notes" in intermediate and st.checkbox("**4.** Final Optimized ANSI SQL Explanation", key=f"optimization_notes_{timestamp_key}"):
                        st.write(intermediate["optimization_notes"])

                    if "final_sql_documentation" in intermediate and st.checkbox("**5.** Final SQL Query Documentation", key=f"final_sql_documentation_{timestamp_key}"):
                        st.write(intermediate["final_sql_documentation"])

                    if "performance_metrics" in intermediate:# and st.checkbox("Performance Metrics Comparison", key=f"performance_metrics_box_{timestamp_key}"):
                        st.subheader("📊 Performance Metrics")
                        if intermediate["performance_metrics"]:
                            metrics_df = pd.DataFrame(intermediate["performance_metrics"])

                            if set(["Databricks (Original)", "Databricks (Optimized)"]).issubset(metrics_df.columns):
                                metrics_df = metrics_df[["KPI", "Snowflake (Original)", "Snowflake (Optimized)", "Databricks (Original)", "Databricks (Optimized)"]]

                                if "index" in metrics_df.columns:
                                    metrics_df = metrics_df.drop(columns=["index"])

                                # Move KPI to index
                                metrics_df = metrics_df.set_index("KPI")

                                # Round numbers
                                metrics_df = metrics_df.round(2)

                                # Format nicely
                                styled_metrics_df = metrics_df.style.format("{:.2f}")

                                # Display clean table
                                st.table(styled_metrics_df)
                            else:
                                st.info("No performance metrics available.")
                                
                if "validation_result" in intermediate:
                        validation_result = intermediate["validation_result"]
                        if validation_result.get("validation_status") == "success":
                            st.success("Final Validation Verdict: Code outputs match in both source platform (Snowflake) and destination platform (Databricks)")
                        else:
                            st.error("Validation Result: Validation failed!")
                            for issue in validation_result.get("failed_checks", []):
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
            with st.expander("View all result"):
                if "AST" in intermediate_results and st.checkbox("**1.** Intermediary Code Logic Tree", key=f"AST_{timestamp_key}"):
                    st.json(intermediate_results["AST"])
                if "translated_ansi_sql" in intermediate_results and st.checkbox("**2.** Translated destination platform code (ANSI SQL)", key=f"translated_ansi_sql_{timestamp_key}"):
                    st.code(intermediate_results["translated_ansi_sql"], language="sql")
                if "join_agg_optimized_sql" in intermediate_results and st.checkbox("**3a.** Join SQL Code Optimization AI Agent Output",key=f"join_agg_optimized_sql_{timestamp_key}"):
                    st.code(intermediate_results["join_agg_optimized_sql"], language="sql")
                if "simplified_sql" in intermediate_results and st.checkbox("**3b.** Simplification AI Agent Output", key=f"simplified_sql_{timestamp_key}"):
                    st.code(intermediate_results["simplified_sql"], language="sql")
                if "filtered_sql" in intermediate_results and st.checkbox("**3c.** Efficient Filtering AI Agent Output", key=f"filtered_sql_{timestamp_key}"):
                    st.code(intermediate_results["filtered_sql"], language="sql")
                if "optimization_notes" in intermediate_results and st.checkbox("**4.** Final Optimized ANSI SQL Explanation", key=f"optimization_notes_{timestamp_key}"):
                    st.write(intermediate_results["optimization_notes"])
                if "final_sql_documentation" in intermediate_results and st.checkbox("**5.** Final SQL Query Documentation", key=f"final_sql_documentation_{timestamp_key}"):
                        st.write(intermediate_results["final_sql_documentation"])
                if "performance_metrics" in intermediate_results:# and st.checkbox("Performance Metrics Comparison", key=f"performance_metrics_box_{timestamp_key}"):
                        st.subheader("📊 Performance Metrics")
                        if intermediate_results["performance_metrics"]:
                            metrics_df = pd.DataFrame(intermediate_results["performance_metrics"])

                            if set(["Databricks (Original)", "Databricks (Optimized)"]).issubset(metrics_df.columns):
                                metrics_df = metrics_df[["KPI", "Snowflake (Original)", "Snowflake (Optimized)", "Databricks (Original)", "Databricks (Optimized)"]]

                                if "index" in metrics_df.columns:
                                    metrics_df = metrics_df.drop(columns=["index"])

                                # Move KPI to index
                                metrics_df = metrics_df.set_index("KPI")

                                # Round numbers
                                metrics_df = metrics_df.round(2)

                                # Format nicely
                                styled_metrics_df = metrics_df.style.format("{:.2f}")

                                # Display clean table
                                st.table(styled_metrics_df)
                            else:
                                st.info("No performance metrics available.")

            if "validation_result" in intermediate_results:
                        validation_result = intermediate_results["validation_result"]
                        if validation_result.get("validation_status") == "success":
                            st.success("Final Validation Verdict: Code outputs match in both source platform (Snowflake) and destination platform (Databricks)")
                        else:
                            st.error("Validation Result: Validation failed!")
                            for issue in validation_result.get("failed_checks", []):
                                st.markdown(f"- **{issue['check']}**: {issue['reason']}")

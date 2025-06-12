import os
import json
import streamlit as st
from langchain_openai import ChatOpenAI
from utils import ConverterState, parse_final_optimised_query
from .query_processor_prompts import parse_sql_to_ast_prompt, translate_ast_to_ansi_prompt, validate_ansi_sql_prompt, optimize_joins_aggregations_prompt, optimize_simplify_query_prompt, optimize_data_filtering_prompt, coordinate_results_prompt, document_final_sql_prompt
<<<<<<< HEAD
from dotenv import load_dotenv
# Load environment variables from a .env file
load_dotenv()
=======

>>>>>>> origin/main
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

llm = ChatOpenAI(
    temperature=0,
    model_name="gpt-4o",  
    streaming=False
)

def parse_sql_to_ast(state: ConverterState) -> dict:

    with st.spinner("Thinking..."):
        query = state["input_query"]
        
        user_message = f"SQL to parse:\n{query}"

        response = llm.invoke(
            [ 
                {"role": "system", "content": parse_sql_to_ast_prompt},
                {"role": "user", "content": user_message},
            ]
        )

        try:
            ast_data = json.loads(response.content)
        except Exception as e:
            ast_data = {"error": str(e), "raw_response": response.content}

        return {
            "ast": ast_data
        }

def translate_ast_to_ansi(state: ConverterState) -> dict:

    with st.spinner("Translating Snowflake SQL to ANSI SQL..."):
        ast_data = state["ast"]
        original_sql = state["input_query"]
        
        user_message = (
            "Original Snowflake SQL:\n"
            f"{original_sql}\n\n"
            "AST:\n"
            f"{json.dumps(ast_data, indent=2)}"
        )

        response = llm.invoke(
            [
                {"role": "system", "content": translate_ast_to_ansi_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        ansi_sql = response.content.strip()

        return {
            "translated_sql": ansi_sql
        }

def validate_ansi_sql(state: ConverterState) -> dict:

    with st.spinner("Validating ANSI SQL..."):
        candidate_sql = state["translated_sql"]
        original_sql = state["input_query"]

        user_message = (
            "Original Snowflake SQL:\n"
            f"{original_sql}\n\n"
            "ANSI SQL:\n"
            f"{candidate_sql}"
        )

        response = llm.invoke(
            [
                {"role": "system", "content": validate_ansi_sql_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        translated_ansi_sql = response.content.strip()

        return {
            "translated_sql": translated_ansi_sql
        }

def optimize_joins_aggregations(state: ConverterState) -> dict:
    """
    Optimizes joins and aggregations in the SQL query to improve performance.

    Args:
        state (ConverterState): The current state containing the validated SQL query

    Returns:
        dict: Dictionary containing the optimized SQL query
    """
    with st.spinner("Optimizing..."):
        # Extract the current SQL from the state
        translated_sql = state["translated_sql"]
        user_message = (
            "SQL Query to Optimize:\n"
            f"{translated_sql}\n\n"
            "Please optimize the joins and aggregations in this query to improve performance while maintaining the exact same results."
        )

        response = llm.invoke(
            [
                {"role": "system", "content": optimize_joins_aggregations_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        optimized_sql = response.content.strip()

        return {
            "join_agg_optimized_sql": optimized_sql
        }

def optimize_simplify_query(state: ConverterState) -> dict:
    """
    Simplifies and streamlines SQL queries by removing redundancies, optimizing structure,
    and eliminating unnecessary elements while preserving functionality.

    Args:
        state (ConverterState): The current state containing the validated SQL query

    Returns:
        dict: Dictionary containing the simplified SQL query
    """
    with st.spinner("Optimizing..."):
        # Extract the current SQL from the state
        translated_sql = state["translated_sql"]
        user_message = (
            "SQL Query to Simplify:\n"
            f"{translated_sql}\n\n"
            "Please simplify this query by removing unnecessary elements, optimizing structure, and improving overall efficiency while maintaining the exact same results."
        )

        response = llm.invoke(
            [
                {"role": "system", "content": optimize_simplify_query_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        simplified_sql = response.content.strip()

        return {
            "simplified_sql": simplified_sql
        }

def optimize_data_filtering(state: ConverterState) -> dict:
    """
    Optimizes SQL queries by improving data filtering techniques to reduce the amount
    of data processed and ensure efficient index usage.

    Args:
        state (ConverterState): The current state containing the validated SQL query

    Returns:
        dict: Dictionary containing the optimized SQL query with improved filtering
    """
    with st.spinner("Optimizing..."):
        # Extract the current SQL from the state
        translated_sql = state["translated_sql"]
        user_message = (
            "SQL Query to Optimize Filtering:\n"
            f"{translated_sql}\n\n"
            "Please optimize this query's data filtering approaches to improve performance while maintaining the exact same results. Focus on making filters more efficient, index-friendly, and applied as early as possible in the execution process."
        )

        response = llm.invoke(
            [
                {"role": "system", "content": optimize_data_filtering_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        filtered_sql = response.content.strip()

        return {
            "filtered_sql": filtered_sql
        }

def coordinate_results(state: ConverterState) -> dict:
    """
    Reviews, merges, and reconciles optimized versions of SQL queries from multiple specialist agents
    to produce the best-transformed final query.

    Args:
        state (ConverterState): The current state containing optimized SQL queries from different agents

    Returns:
        dict: Dictionary containing the final optimized SQL query
    """
    with st.spinner("Optimizing..."):
        # Extract the optimized queries from each specialist agent
        original_sql = state["translated_sql"]  # The validated SQL from SyntaxValidatorAgent
        join_agg_sql = state.get("join_agg_optimized_sql", original_sql)  # JoinAggregationOptimizerAgent output
        simplified_sql = state.get("simplified_sql", original_sql)  # QuerySimplificationAgent output
        filtered_sql = state.get("filtered_sql", original_sql)  # DataFilteringAgent output

        user_message = (
            "I need you to coordinate and merge the following optimized versions of the same SQL query into the best possible final version:\n\n"
            "Original SQL Query (after syntax validation):\n"
            f"{original_sql}\n\n"
            "Join/Aggregation Optimized SQL:\n"
            f"{join_agg_sql}\n\n"
            "Query Simplified SQL:\n"
            f"{simplified_sql}\n\n"
            "Data Filtering Optimized SQL:\n"
            f"{filtered_sql}\n\n"
            "Please analyze all versions, resolve any conflicts, and produce a single, highly optimized SQL query that incorporates the best aspects of each specialized version."
        )

        response = llm.invoke(
            [
                {"role": "system", "content": coordinate_results_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        final_optimized_sql = response.content.strip()
        final_query, notes = parse_final_optimised_query(final_optimized_sql)

        return {
            "final_optimized_sql": final_query,
            "optimization_notes": notes
        }

def document_final_sql(state: ConverterState) -> dict:
    """
    Analyzes and documents the final optimized SQL query in a clear, step-by-step, and structured format.
    The output is tailored to be understandable and actionable by both business and technical audiences.

    Args:
        final_optimized_sql (str): The final, reconciled, and optimized SQL query.

    Returns:
        dict: A dictionary containing a comprehensive breakdown and documentation of the SQL query.
    """
    with st.spinner("Documenting..."):
        final_optimized_sql = state.get("final_optimized_sql")
        print('In documentation:', final_optimized_sql)
        user_message = (
            "Please analyze the following final optimized SQL query and convert it into well-organized, step-by-step documentation suitable for both technical and business stakeholders:\n\n"
            f"Final Optimized SQL Query:\n{final_optimized_sql}\n\n"
            "Your output should be structured according to the documentation guidelines above."
        )
    
        response = llm.invoke(
            [
                {"role": "system", "content": document_final_sql_prompt},
                {"role": "user", "content": user_message},
            ]
        )
        documentation = response.content.strip()
        print('SQL Documentation:', documentation)
        
    return {
        "final_sql_documentation": documentation
    }
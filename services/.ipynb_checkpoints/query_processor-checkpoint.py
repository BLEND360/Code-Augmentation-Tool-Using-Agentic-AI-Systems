import os
import json
import streamlit as st
from langchain_openai import ChatOpenAI
from utils import ConverterState, parse_efficient_query_json
from .query_processor_prompts import parse_sql_to_ast_prompt, translate_ast_to_ansi_prompt, validate_ansi_sql_prompt, efficient_ansi_sql_prompt

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

def efficient_ansi_sql(state: ConverterState) -> dict:

    with st.spinner("Converting to efficient ANSI SQL..."):
        validated_ansi_sql = state["translated_sql"]

        response = llm.invoke(efficient_ansi_sql_prompt.format(sql_query=validated_ansi_sql))


        efficient_ansi_sql = parse_efficient_query_json(response.content)

        return {
            "efficient_sql": efficient_ansi_sql.get('sql_query', ""),
            "messages": efficient_ansi_sql.get('steps', [])
        }

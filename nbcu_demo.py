import json
import streamlit as st
from langgraph.graph import StateGraph, END
from utils import ConverterState
from services.query_processor import parse_sql_to_ast, translate_ast_to_ansi, validate_ansi_sql, efficient_ansi_sql

def convert_snowflake_to_ansi(sql_query: str):

    intermediate_results = {}
   
    initial_state = ConverterState(
        input_query=sql_query,
        ast=None,
        translated_sql="",
        efficient_sql="",
        messages=[] 
    )

    final_state = app.invoke(initial_state)
    intermediate_results["AST"] = final_state.get("ast", {})
    intermediate_results["translated_ansi_sql"] = final_state.get("translated_sql", "")
    intermediate_results["Efficient_sql_steps"] = final_state.get("messages", [])
    
    return final_state["efficient_sql"], intermediate_results

workflow = StateGraph(ConverterState)
workflow.add_node("ParserAgent", parse_sql_to_ast)
workflow.add_node("TranslationAgent", translate_ast_to_ansi)
workflow.add_node("SyntaxValidatorAgent", validate_ansi_sql)
workflow.add_node("EfficientQueryAgent", efficient_ansi_sql)

workflow.set_entry_point("ParserAgent")

workflow.add_edge("ParserAgent", "TranslationAgent")
workflow.add_edge("TranslationAgent", "SyntaxValidatorAgent")
workflow.add_edge("SyntaxValidatorAgent", "EfficientQueryAgent")
workflow.add_edge("EfficientQueryAgent", END)
app = workflow.compile()

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

# App title
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
                with st.expander("View Intermediate Steps", expanded=False):  # Default not expanded
                    for step_name, step_result in chat["intermediate"].items():
                        if step_name == "AST":
                            st.subheader("AST Tree")
                            st.json(step_result)  # Display AST as JSON
                        elif step_name == "translated_ansi_sql":
                            st.subheader("Translated ANSI SQL")
                            st.code(step_result, language="sql")  # Display SQL code in a formatted way
                        elif step_name == "Efficient_sql_steps":
                            st.subheader("Efficient SQL Steps")
                            for message in step_result:
                                st.write(f"- {message}")  # Display each message as a bullet point

# Chat input
user_question = st.chat_input("Type your Snowflake SQL query...")
if user_question:
    # Display the user input
    with st.chat_message("user"):
        st.text(user_question)

    try:
        ansi_result, intermediate_results = convert_snowflake_to_ansi(user_question)
        success = True
    except Exception as e:
        success = False
        intermediate_results = {}
        ansi_result = f"Error: {e}"

    # Save the result to chat history
    chat_entry = {"question": user_question, "answer": ansi_result, "intermediate": intermediate_results}
    st.session_state.interactive_chat_history.append(chat_entry)
    #st.session_state.interactive_chat_history.append((user_question, ansi_result))

    with st.chat_message("assistant"):
        if success:
            st.code(ansi_result, language="sql")  # Display ANSI SQL as a code block
        else:
            st.error(ansi_result)  # Display error message

        # Show intermediate results
        if success and intermediate_results:
            with st.expander("View Intermediate Steps"):
                for step_name, step_result in intermediate_results.items():
                    if step_name == "AST":
                        st.subheader("AST Tree")
                        st.json(step_result)  # Display AST as JSON
                    elif step_name == "translated_ansi_sql":
                        st.subheader("Translated ANSI SQL")
                        st.code(step_result, language="sql")  # Display SQL code in a formatted way
                    elif step_name == "Efficient_sql_steps":
                        st.subheader("Efficient SQL Steps")
                        for message in step_result:
                            st.write(f"- {message}")  # Display each message as a bullet point

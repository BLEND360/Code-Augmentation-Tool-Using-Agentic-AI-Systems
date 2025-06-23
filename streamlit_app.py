import streamlit as st
from pages.intro import render as render_intro
from pages.codeaug import render as render_codeaug
import os




# Set page configuration
st.set_page_config(page_title="Code Augmentation Tool", layout="wide")

# Custom CSS to style the buttons and hide unnecessary sidebar items
st.markdown(
    """
    <style>
    /* Hide the Streamlit app menu and unnecessary links */
    [data-testid="stSidebarNav"] {
        display: none;
    }
    
    /* Style the sidebar buttons */
    [data-testid="stSidebar"] .stButton button {
        width: 220px;
        background-color: #f0f0f0;
        border: 1px solid #ccc;
        border-radius: 5px;
        font-size: 16px;
        padding: 10px;
        margin-top: 10px;
        text-align: center;
        
    }
    [data-testid="stSidebar"] .stButton button:hover {
        background-color: #ddd;
        border-color: #888;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Sidebar Navigation
st.sidebar.title("Code Augmentation Tool")
if "current_page" not in st.session_state:
    st.session_state.current_page = "Introduction"

# Sidebar buttons for navigation
if st.sidebar.button("Introduction"):
    st.session_state.current_page = "Introduction"
if st.sidebar.button("Code Augmentation"):
    st.session_state.current_page = "Code Augmentation"


# Page Routing
if st.session_state.current_page == "Introduction":
    render_intro()
elif st.session_state.current_page == "Code Augmentation":
    render_codeaug()

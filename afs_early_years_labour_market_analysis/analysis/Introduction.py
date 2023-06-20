import streamlit as st
import os
from pathlib import Path

st.set_page_config(
    page_title="",
    page_icon="👋",
)

PROJECT_DIR = Path(__file__).resolve().parents[1]
asf_folder = os.path.join(PROJECT_DIR, "analysis/")

with open(os.path.join(asf_folder, "style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)


st.markdown(
    "<b>Welcome to analysis of the early years labour market 👋</b>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    We analyse both current shortages at a regional- and local-authority level, and future shortages at a regional-level level.
"""
)

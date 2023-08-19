import sys

sys.path.append(
    "/Users/india.kerlenesta/Projects/afs_early_years_labour_market_analysis"
)

import streamlit as st
import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]
images_folder = os.path.join(PROJECT_DIR, "analysis/images")

st.set_page_config(
    page_title="EYP Current and Future shortages",
    page_icon=os.path.join(images_folder, "nesta_logo.png"),
    layout="centered",
)

with open(os.path.join(PROJECT_DIR, "analysis/style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

st.write("# Introduction")

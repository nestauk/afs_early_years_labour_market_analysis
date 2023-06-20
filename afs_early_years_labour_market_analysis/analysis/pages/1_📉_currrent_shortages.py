import streamlit as st
from pathlib import Path

import os

PROJECT_DIR = Path(__file__).resolve().parents[1]

with open(os.path.join(str(PROJECT_DIR), "style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)


# ========================================
# ---------- Load Data ------------

st.title("Early Years Staff - Current Shortfall")

st.markdown(
    "There is no one source to look at in order to understand the current shortfall in the sector, and we are unlikely to be able to arrive at a specific number for the number of staff currently needed using administrative sources."
)

st.title("Current shortfall in the sector: Administrative sources")

st.title("Current shortfall in the sector: job advert data")
st.markdown(
    """analysis to include:
- number of job adverts for early years staff
- number of job adverts for early years staff by region
- compare demand for early years staff to demand for other staff/comparable roles
- regionally compare demand for early years staff (+ regional deprivation using IMD)
- top skills, salary differences and historical trends since we started collecting data in 2020
            """
)

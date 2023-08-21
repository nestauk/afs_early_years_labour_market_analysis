import os
from pathlib import Path

import pandas as pd
import numpy as np
import altair as alt
import random
import streamlit as st
import json

import analysis_utils as au

# ==================================Variables & Set Up ===========================================

alt.data_transformers.disable_max_rows()
random.seed(42)

chart_title_font_size = 20

PROJECT_DIR = Path(__file__).resolve().parents[1]
data_folder = os.path.join(PROJECT_DIR, "data")
images_folder = os.path.join(PROJECT_DIR, "images")

# ==================================Load data function ===================================


# weird but wrapped so i can cache the loading data function
@st.cache_data
def load_data():
    (
        monthly_profession_mean,
        monthly_profession_sal_count,
        qualification_level_salary_boxplot,
        salary_by_profession_boxplot,
        skill_profile_similarity,
        top_skills_per_profession_and_nation,
        wage_ratio_df,
        median_salary_count_per_profession_table,
        median_salary_count_per_qual_table,
    ) = au.load_analysis_data(data_folder)

    return (
        monthly_profession_mean,
        monthly_profession_sal_count,
        qualification_level_salary_boxplot,
        salary_by_profession_boxplot,
        skill_profile_similarity,
        top_skills_per_profession_and_nation,
        wage_ratio_df,
        median_salary_count_per_profession_table,
        median_salary_count_per_qual_table,
    )


# load data
(
    monthly_profession_mean,
    monthly_profession_sal_count,
    qualification_level_salary_boxplot,
    salary_by_profession_boxplot,
    skill_profile_similarity,
    top_skills_per_profession_and_nation,
    wage_ratio_df,
    median_salary_count_per_profession_table,
    median_salary_count_per_qual_table,
) = load_data()

# define options
nations = ["England", "Scotland", "Wales"]
professions = [
    "Early Years Practitioner",
    "Waiter",
    "Primary School Teacher",
    "Retail Assistant",
    "Secondary School Teacher",
]

# ==================================Streamlit App======================================

with open(os.path.join(PROJECT_DIR, "style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

# ------ Introduction -------

st.header("", anchor="introduction")
intro_text = """
We use job postings data to identify shortages in the Early Years Practitioner (EYP) jobs.

We primarily look salaries as a **proxy for demand**.

"""

st.markdown("<p class='medium-font'>Current Shortages</p>", unsafe_allow_html=True)
st.markdown(intro_text)

nation = st.radio("Pick a nation", (nations), horizontal=True, key="1")

# ------ Salaries within EYP -------

salaries_eyp_test = """
Graphs about EYP salaries per qual level here.
"""

st.markdown("<p class='medium-font'>EYP Salaries</p>", unsafe_allow_html=True)
st.markdown(salaries_eyp_test)


# qualification level salary boxplot
metric1, metric2 = st.columns((1, 1))

eyp_min_sal_value = (
    median_salary_count_per_profession_table.query(
        "profession == 'Early Years Practitioner'"
    )
    .query(f"nation == '{nation}'")
    .inflation_adj_min_salary.values[0]
)
metric1.metric(
    label=f"*Median Minimum Annual EYP Salary in {nation} (£)*",
    value=f"£{eyp_min_sal_value}",
)

eyp_max_sal_value = (
    median_salary_count_per_profession_table.query(
        "profession == 'Early Years Practitioner'"
    )
    .query(f"nation == '{nation}'")
    .inflation_adj_max_salary.values[0]
)
metric2.metric(
    label=f"*Median Maximum Annual EYP Salary (£) in {nation}*",
    value=f"£{eyp_max_sal_value}",
)


box_plot_df = qualification_level_salary_boxplot.query(f'nation == "{nation}"').drop(
    columns=["nation"]
)
box_plot_graph = au.generate_boxplot(box_plot_df, "Qualification Level", 0)

st.altair_chart(box_plot_graph, use_container_width=True)

salaries_text = f"""
More text about salaries here.
"""
st.markdown(salaries_text, unsafe_allow_html=True)

# wage ratio
wage_ratio_df = wage_ratio_df.query(f'nation == "{nation}"').drop(columns=["nation"])
qual_graph = au.generate_wage_ratio(wage_ratio_df)

st.altair_chart(
    qual_graph,
    use_container_width=True,
)

# add skill similarity graph
skill_profile_similarity = skill_profile_similarity.query(f'nation == "{nation}"').drop(
    columns=["nation"]
)
skill_sims = au.generate_skill_similarity(skill_profile_similarity)
skill_sims_configured = au.configure_plots(
    skill_sims,
    chart_title=f"EYP Skill Profile Similarity in {nation}",
    chart_subtitle=[""],
)

st.altair_chart(skill_sims_configured, use_container_width=True)

# ------ Comparison between EYP and other professions: Salaries and Count -------

sal_count_text = """
Graphs about comparing EYP to similar sectors.
"""

st.markdown(
    "<p class='medium-font'>Comparing EYP to similar sectors</p>",
    unsafe_allow_html=True,
)
st.markdown(sal_count_text)

prof = st.selectbox("Pick a profession", professions, key="2")

# add salary over time
monthly_profession_sal_count_melt_filtered = (
    monthly_profession_sal_count.query(f'profession == "{prof}"')
    .query(f'nation == "{nation}"')
    .drop(columns=["profession", "nation"])
)
pro_sal_ts = au.generate_profession_ts_salary(
    monthly_profession_sal_count_melt_filtered, streamlit=True
).properties(title=f"Median monthly salary of {prof}s in {nation} over time")
st.altair_chart(pro_sal_ts, use_container_width=True)

# + sal box plot
salary_by_profession_boxplot_filtered = (
    salary_by_profession_boxplot.query(f'profession == "{prof}"')
    .query(f'nation == "{nation}"')
    .drop(columns=["nation"])
)
# just do it in the script to account for width/height
prof_boxplot = (
    alt.Chart(
        salary_by_profession_boxplot_filtered,
        title=f"Salary Distibution of {prof}s in {nation}",
    )
    .mark_boxplot(size=50, extent=0.5, outliers=False)
    .encode(
        alt.Y("salary:Q", title="Annualised Salary (£)", scale=alt.Scale(zero=False)),
        alt.X("salary_type:N", title="Salary Type"),
        alt.Color("salary_type:N", title="Salary Type"),
    )
)
st.altair_chart(prof_boxplot, use_container_width=True)

# + count over time graph
monthly_profession_mean_filtered = (
    monthly_profession_mean.query(f'profession == "{prof}"')
    .query(f'nation == "{nation}"')
    .drop(columns=["nation"])
)
monthly_profession_mean_ts = au.generate_profession_ts_count(
    monthly_profession_mean_filtered, streamlit=True
).properties(
    title=f"Rolling monthly average # of {prof} job adverts in {nation} over time"
)
st.altair_chart(monthly_profession_mean_ts, use_container_width=True)

# add top skills based on selected profession and EYP

top_skills_text = """Top skills text goes here."""

st.markdown(top_skills_text)

top_skills_per_profession_and_nation_nation = (
    top_skills_per_profession_and_nation.query(f'nation == "{nation}"').drop(
        columns=["nation"]
    )
)
prof_barchart = au.generate_top_skills_barchart(
    top_skills_per_profession_and_nation_nation, prof
)
st.altair_chart(prof_barchart, use_container_width=True)

"""
OJO Analysis streamlit page
"""

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

column_type_dict = {
    "count": "# of Job Postings",
    "median_max_annualised_salary": "Salary",
    "median_min_annualised_salary": "Salary",
}

alt.data_transformers.disable_max_rows()
random.seed(42)

chart_title_font_size = 20

PROJECT_DIR = Path(__file__).resolve().parents[1]
data_folder = os.path.join(PROJECT_DIR, "data")
images_folder = os.path.join(PROJECT_DIR, "images")

st.set_page_config(
    page_title="EYP Current and Future shortages",
    page_icon=os.path.join(images_folder, "nesta_logo.png"),
)

# ==================================Load data function ===================================

# load data functions


@st.cache_data
def load_ojo_analysis_data():
    sal_count_sector = pd.read_csv(
        os.path.join(data_folder, "sector_sal_count_over_time.csv")
    )
    #read json file from local directory
    eyp_metadata = json.load(open(os.path.join(data_folder, "eyp_job_metadata.json")))
    
    qual_data = pd.read_csv(os.path.join(data_folder, "qual_data.csv"))

    return sal_count_sector, eyp_metadata, qual_data


# ==================================Graph functions======================================

# load data
sal_count_sector, eyp_metadata, qual_data = load_ojo_analysis_data()

def make_over_time_graph(
    df_jobs: pd.DataFrame = sal_count_sector,
    job_type: str = "Early Year Practitioner",
    column_type: str = "count",
):
    """Generates an interactive graph of job adverts over time

    Args:
        df_jobs (pd.DataFrame): Job adverts dataframe
        job_type (str): Name of job title to be used in graph title
        column_type (str): The variable to graph

    Returns:
        alt.vegalite.v5.api.Chart: Interactive graph of job adverts over time
    """
    column_type_name = column_type_dict.get(column_type)
    df_jobs_filtered = df_jobs.query(f'sector == "{job_type}"')

    base = (
        alt.Chart(df_jobs_filtered)
        .mark_line()
        .encode(
            x=alt.X("month_year:T", title="Year"),
            y=alt.Y(f"{column_type}:Q", title=column_type_name),
            strokeWidth=alt.value(4),
            color=alt.value(random.choice(au.NESTA_COLOURS)),
            tooltip=[alt.Tooltip(column_type, title=column_type_name)],
        )
    )
    
    if column_type_name == "Salary":
        #add line for the median UK salary in 2023
        hline = (
                alt.Chart(pd.DataFrame({column_type: [33000], "color": ["red"]}))
                .mark_rule(opacity=0.8, strokeDash=[5, 5])
                .encode(y=column_type, color=alt.Color("color:N", scale=None))
            )

        base = base + hline
    
    return (
    au.configure_plots(base)
    .properties(width=600, height=400)
    .interactive())

def make_qualification_graph(source: pd.DataFrame = qual_data):
    """Make qualification graph

    Args:
        source (pd.DataFrame, optional): Qualifications dataFrame. Defaults to qual_data.

    Returns:
        alt.vegalite.v5.api.Chart: Interactive graph of qualification information 
    """

    qual_sorted = ['6', '5', '4', '3', '2', '1']

    qual_count = alt.Chart(source).mark_bar().encode(
        #sort values by count in descending order
        x=alt.X('count', title="# of Job Adverts", type="quantitative"),
        y=alt.Y('Qualification Level', sort=qual_sorted, type="ordinal"),
        color=alt.Color('degree_not', title='Requires Degree', scale=alt.Scale(domain=['Yes', 'No'], range=[au.NESTA_COLOURS[1], au.NESTA_COLOURS[2]])),
        tooltip=[alt.Tooltip('Qualification Level'), alt.Tooltip('count', title='# of Job Adverts')]
    )

    qual_wage_ratio = alt.Chart(source).mark_bar().encode(
        #sort values by count in descending order
        x=alt.X('wage_ratio', title= "Wage Ratio", type="quantitative"),
        y=alt.Y('Qualification Level', sort=qual_sorted, title=None, axis=None, type="ordinal"),
        color=alt.Color('degree_not', title='Requires Degree', scale=alt.Scale(domain=['Yes', 'No'], range=[au.NESTA_COLOURS[1], au.NESTA_COLOURS[2]])),
        tooltip=[alt.Tooltip('Qualification Level'), alt.Tooltip('median_salary', title='Median Annual Salary')]
    )

    qual_graph = qual_count | qual_wage_ratio
    
    return au.configure_plots(qual_graph)

# ==================================Streamlit App======================================

with open(os.path.join(PROJECT_DIR, "style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

# ------ Introduction -------

st.header("", anchor="introduction")
intro_text = """
We use job postings data to identify shortages in the Early Years Practitioner (EYP) jobs.

We primarily look at the change in # of job postings and salaries as a **proxy for demand**.

"""

st.markdown("<p class='medium-font'>Current Shortages</p>", unsafe_allow_html=True)
st.markdown(intro_text)

# ------ Within EYP -------

st.header("", anchor="within_eyp")

sum_text = f"""
This is filler text for the eyp section. Include:

- summary stats
    - There are {eyp_metadata['no_jobs']} Early Year Practitioner job postings in the UK between **{eyp_metadata['job_adverts_range'][0]}** and **{eyp_metadata['job_adverts_range'][0]}**. The annualised salary range is between **£{round(eyp_metadata['max_sal_info']['min'])}** and **£{round(eyp_metadata['max_sal_info']['max'])}**.
- qualification level observations 
"""

st.markdown(
    "<p class='medium-font'>Early Year Practitioners Deep Dive</p>",
    unsafe_allow_html=True,
)

st.markdown(sum_text, unsafe_allow_html=True)
    
qual_chart = make_qualification_graph()

# # Display the chart using Streamlit
st.altair_chart(qual_chart, use_container_width=True)

# ------ Across Sector comparison -------

st.header("", anchor="across_eyp")

sum_text = """
We identify 'similar' occupations based on the skills profile of different job postings. Specifically,
we employ the [Mapping Career Causeways algorithm](https://github.com/nestauk/mapping-career-causeways) to
identify similar jobs based on the similarity in skill profiles across occupations and manually add
additional occupations flagged as similar by subject matter experts.
"""

st.markdown(
    "<p class='medium-font'>Demand Across Similar Sectors</p>", unsafe_allow_html=True
)

st.markdown(sum_text)

sectors = list(sal_count_sector.sector.unique())
sector_group = st.selectbox(
    "Select a sector", sectors, index=1
)

metric1, metric2 = st.columns((1, 1))
avg_sal = round(
    sal_count_sector.query(
        f'sector == "{sector_group}"'
    ).median_max_annualised_salary.median()
)

metric1.metric(label="*Median Annualised Salary*", value=f"£{avg_sal}")

metric2.metric(
    label="*Average # of Postings per Year*",
    value=round(sal_count_sector.query(f'sector == "{sector_group}"')["count"].mean()),
)

between_sectors_text = """
The graph on the right shows the rolling monthly number of job postings while the
graph on the left shows the monthly median maximum annualised salary of job postings between 2020 and 2023.

While some similar sectors have seen a slight increase in salary over time, most have remained stable. The red dotted line indicates the median full time UK salary across all occupations in 2023 (£33,000).
"""
st.markdown(between_sectors_text)

count_over_time_by_sector = make_over_time_graph(
    job_type=sector_group, column_type="count"
)

column_type = "median_max_annualised_salary"
salary_over_time_by_sector = make_over_time_graph(
    job_type=sector_group, column_type=column_type
)

col1, col2 = st.columns([50, 50])

with col1:
    st.altair_chart(count_over_time_by_sector, use_container_width=True)
with col2:
    st.altair_chart(salary_over_time_by_sector, use_container_width=True)

between_sectors_demand = """
We can look look at change in salary or # of job postings for similar sectors as a proxy for 
demand. 
"""
st.markdown(between_sectors_demand)

demand_type = st.selectbox("Select a demand type", ["# of Postings", "Salary", "Skills"], index=0)
"""
OJO Analysis
"""

import os
from pathlib import Path

import pandas as pd
import numpy as np
import altair as alt
import random

import analysis_utils as au
import streamlit as st

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
    page_title="EYP Shortages", page_icon=os.path.join(images_folder, "nesta_logo.png")
)

# ==================================Load data function ===================================

# load data functions


@st.cache(allow_output_mutation=True)
def load_ojo_analysis_data():
    sal_count_sector = pd.read_csv(
        os.path.join(data_folder, "sector_sal_count_over_time.csv")
    )
    qual_count_sal = pd.read_csv(
        os.path.join(data_folder, "qual_sal_count_over_time.csv")
    )
    qual_sal = pd.read_csv(os.path.join(data_folder, "sal_by_qual_level.csv"))

    return sal_count_sector, qual_count_sal, qual_sal


# ==================================Graph functions======================================

# load data
sal_count_sector, qual_count_sal, qual_sal = load_ojo_analysis_data()

## Summary statistics graphs


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

    over_time_graph = (
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

    return (
        au.configure_plots(over_time_graph)
        .properties(width=600, height=400)
        .configure_title(fontSize=chart_title_font_size)
        .interactive()
    )


def make_qualification_level_dist_graph():
    """Make salary distribution graph by qualification level

    Returns:
        Salary distribution graph by qualification level
    """

    qual_sal.columns = [str(int(float(i))) for i in list(qual_sal.columns)]

    base = (
        alt.Chart(qual_sal)
        .transform_fold(list(qual_sal.columns), as_=["Qualification Level", "Salary"])
        .mark_bar(opacity=0.3, binSpacing=0)
        .encode(
            alt.X(
                "Salary:Q",
                title="Salary",
                bin=alt.Bin(maxbins=100),
                scale=alt.Scale(domain=(0, 65000)),
            ),
            alt.Y("count()", title="# of Job Postings", stack=None),
            alt.Color("Qualification Level:N"),
        )
    )

    title = "Salary Distribution by Qualification Level"
    return au.configure_plots(base, chart_title=title)


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

sum_text = """
This is filler text for the within eyp demand section.
"""

st.markdown(
    "<p class='medium-font'>Demand for Early Year Practitioners</p>",
    unsafe_allow_html=True,
)

st.markdown(sum_text)

qual_graph = make_qualification_level_dist_graph()

st.altair_chart(qual_graph, use_container_width=True)

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

sector_group = st.selectbox(
    "Select a sector", list(sal_count_sector.sector.unique()), index=1
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

While some similar sectors have seen a slight increase in salary over time, most have remained stable.
"""
st.markdown(between_sectors_text)

count_over_time_by_sector = make_over_time_graph(
    job_type=sector_group, column_type="count"
)
salary_over_time_by_sector = make_over_time_graph(
    job_type=sector_group, column_type="median_max_annualised_salary"
)

col1, col2 = st.columns([50, 50])

with col1:
    st.altair_chart(count_over_time_by_sector, use_container_width=True)
with col2:
    st.altair_chart(salary_over_time_by_sector, use_container_width=True)

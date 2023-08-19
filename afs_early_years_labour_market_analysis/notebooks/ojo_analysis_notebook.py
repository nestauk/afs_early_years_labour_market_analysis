# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     formats: py,ipynb
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.5
#   kernelspec:
#     display_name: dap_prinz_green_jobs
#     language: python
#     name: python3
# ---

import sys

sys.path.append(
    "/Users/india.kerlenesta/Projects/afs_early_years_labour_market_analysis"
)

# This notebook contains the analysis for current staff shortages in early years practitioner job adverts
# and similar professions.

# +
# import relevant libraries
import afs_early_years_labour_market_analysis.getters.ojd_daps as od
import afs_early_years_labour_market_analysis.analysis.analysis_utils as au
from afs_early_years_labour_market_analysis import BUCKET_NAME, PROJECT_DIR
from afs_early_years_labour_market_analysis.getters.data_getters import save_to_s3
from afs_early_years_labour_market_analysis.pipeline.data_enrichment.clean_relevant_jobs import *
from colour import Color

import pandas as pd
import altair as alt
import os

# +
# disable max rows for altair graphs
alt.data_transformers.disable_max_rows()

analysis_data_path = (
    PROJECT_DIR / "afs_early_years_labour_market_analysis/analysis/data"
)
output_table_path = "outputs/ojo_analysis/report_tables/"
# -

# ## 0. Load datasets
# - only include england

# +
# # 0.1 Load whole datasets for figures to state in report

print("Loading relevant datasets...")
eyp_jobs_clean = pd.read_csv(
    "s3://afs-early-years-labour-market-analysis/outputs/curated_data/eyp_jobs_clean.csv"
)
sim_jobs_clean = pd.read_csv(
    "s3://afs-early-years-labour-market-analysis/outputs/curated_data/sim_jobs_clean.csv"
)
all_skills = pd.read_csv(
    "s3://afs-early-years-labour-market-analysis/outputs/curated_data/all_skills_clean.csv"
)
all_jobs_clean = pd.concat([eyp_jobs_clean, sim_jobs_clean])

# #0.2 Load analysis datasets
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
) = au.load_analysis_data(analysis_data_path)
# -

# ## 1. Print facts and figures for the report
# - print figures to state in the report

# +
# Print figures to include in the report

eyp_jobs_clean_england = eyp_jobs_clean.query("nation == 'England'")

print(
    f"there are {len(eyp_jobs_clean_england)} job adverts in the EYP profession in England between {eyp_jobs_clean_england.created.min()} and {eyp_jobs_clean_england.created.max()}"
)
print("")
print(
    f"the minimum annualised salary range is between £{eyp_jobs_clean_england.inflation_adj_min_salary.min()} and £{eyp_jobs_clean_england.inflation_adj_min_salary.max()}"
)
print("")
print(
    f"the median minimum annualised salary is  £{eyp_jobs_clean_england.inflation_adj_min_salary.median()}"
)
print("")
print(
    f"the median maximum annualised salary is  £{eyp_jobs_clean_england.inflation_adj_max_salary.median()}"
)
print("")
print(
    f"the maximum annualised salary range is between £{eyp_jobs_clean_england.inflation_adj_max_salary.min()} and £{eyp_jobs_clean_england.inflation_adj_max_salary.max()}"
)
print("")
low_sal = eyp_jobs_clean[eyp_jobs_clean.inflation_adj_max_salary < 15000]
print(
    f"{len(low_sal)} or {round((len(low_sal)/len(eyp_jobs_clean))*100, 2)}% of jobs pay less than £15,000 per year"
)
# -

# Print the number of jobs by 'profession'
for profession, profession_info in all_jobs_clean.query('nation == "England"').groupby(
    "profession"
):
    print(f"there are {len(profession_info)} jobs in the {profession} profession")

# +
# Top 10 most common job titles per profession table - for appendix

top_10_titles = (
    all_jobs_clean.groupby("profession")
    .agg({"job_title_raw": "value_counts"})
    .rename(columns={"job_title_raw": "count"})
    .groupby(level=0)
    .head(10)
    .reset_index()
    .rename(columns={"job_title_raw": "job_title"})
)

top_10_titles_path = os.path.join(output_table_path, "top_10_titles_per_profession.csv")
save_to_s3(BUCKET_NAME, top_10_titles, top_10_titles_path)
# -

# ## 2. Generate Graphs
#
# ### 2.1 Graphs related to salaries
#
# Generate:
# - boxplots by qualification level and profession
# - salary timeseries by profession with median England wage line

# +
# Generate Salary Distribution by Qualification Level Graph

box_plot_df = qualification_level_salary_boxplot.query('nation == "England"').drop(
    columns=["nation"]
)
box_plot_graph = au.generate_boxplot(
    box_plot_df, "Qualification Level", 0, streamlit=False
)

sal_qual_boxplot = au.configure_plots(
    box_plot_graph,
    chart_title="Annualised Salary Distribution by EYP Qualification Level",
    chart_subtitle=["Salaries are much higher for those with a degree (level 6)."],
)

sal_qual_boxplot
# -

# Generate Wage Ratio Graph
wage_ratio_df = wage_ratio_df.query('nation == "England"').drop(columns=["nation"])
qual_graph = au.generate_wage_ratio(wage_ratio_df)
au.configure_plots(
    qual_graph,
    chart_title="Wage Ratio by Qualification Level",
    chart_subtitle=[
        "The wage ratio is the ratio of the median salary for a given qualification",
        "level to the median salary for all EYP job adverts.",
    ],
)

# +
# Generate Salary Distribution by profession Graph

box_plot_df = salary_by_profession_boxplot.query('nation == "England"').drop(
    columns=["nation"]
)
box_plot_graph = au.generate_boxplot(box_plot_df, "profession", 5)

sal_sect_boxplot = au.configure_plots(
    box_plot_graph,
    chart_title="Annualised Salary Distribution by Profession",
    chart_subtitle=[
        "The spread in salary varies by profession. Salaries appear most consistant in",
        "early years, retail, waiting and teaching assistant roles.",
    ],
)

sal_sect_boxplot

# +
# Generate Median Salary by profession Over Time Graph
monthly_profession_sal_count_melt = monthly_profession_sal_count.query(
    'nation == "England"'
).drop(columns=["nation"])
median_wage_profession = au.generate_profession_ts_salary(
    monthly_profession_sal_count_melt, streamlit=True
)
sal_sect_ts = au.configure_plots(
    median_wage_profession,
    chart_title="Median Annualised Salary by Profession Over Time",
    chart_subtitle=[
        "Salaries for Early Year Practitioners, Retail Assistants and Waiters have",
        "almost consistently been below England's annual median salary since 2021.",
    ],
)

sal_sect_ts
# -

# ### 2.2 Graphs related to count
#
# Generate:
# - monthly rolling average timeseries by profession

# +
# Generate Rolling Average of Job Adverts Over time by profession Graph
monthly_mean = monthly_profession_mean.query('nation == "England"').drop(
    columns=["nation"]
)
rolling_avg_profession = au.generate_profession_ts_count(monthly_mean)
sect_count_ts = au.configure_plots(
    rolling_avg_profession,
    chart_title="Rolling Monthly Average # of Job Adverts per Profession",
    chart_subtitle=[
        "There appears to be consistant demand across most professions over time with the",
        "exception of Teaching Assistants. This may be due to the seasonality of the role.",
    ],
)

sect_count_ts
# -

# ### 2.3 Graphs related to skills
#
# Generate:
# - a graph on professions with the most similar skill profile
# - top skills at skill level for each profession

# +
# graph on professions with the most similar skill profile

skill_profile_similarity = skill_profile_similarity.query('nation == "England"').drop(
    columns=["nation"]
)
skill_sims = au.generate_skill_similarity(skill_profile_similarity)
au.configure_plots(
    skill_sims, chart_title="Skill Profile Similarity", chart_subtitle=[""]
)

# +
# Generate top skills at the skill level for all jobs

top_skills_per_profession_df = top_skills_per_profession_and_nation.query(
    'nation == "England"'
).drop(columns=["nation"])
charts = []
professions = top_skills_per_profession_df.profession.unique()
for i in range(len(professions)):
    bar_chart = au.generate_top_skills_barchart(
        top_skills_per_profession_df, professions[i]
    )
    charts.append(bar_chart)

top_skills_barchart = alt.vconcat(*charts[:3]) | alt.vconcat(*charts[3:])

au.configure_plots(
    top_skills_barchart,
    chart_title="Top Skills in Each Profession",
    chart_subtitle=[
        " ",
        "There are overlaps in the top skills across all professions.",
        " ",
    ],
)

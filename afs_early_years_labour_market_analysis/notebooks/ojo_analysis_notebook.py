#!/usr/bin/env python
# coding: utf-8

# In[4]:


import sys

sys.path.append(
    "/Users/india.kerlenesta/Projects/afs_early_years_labour_market_analysis/"
)


# This notebook contains the analysis for current staff shortages in early years practitioner job adverts
# and similar sectors.
#
# Demand is proxied by:
# - percent change in # of job adverts;
# - change in salaries.

# In[5]:


# import relevant libraries
import afs_early_years_labour_market_analysis.getters.ojd_daps as od
import afs_early_years_labour_market_analysis.analysis.analysis_utils as au
from afs_early_years_labour_market_analysis import BUCKET_NAME
from afs_early_years_labour_market_analysis.getters.data_getters import save_to_s3

import ojo_analysis_utils as oau
import numpy as np

import pandas as pd
import altair as alt
import os
import ast
import geopandas as gpd

from colour import Color


# In[6]:


# disable max rows for altair graphs
alt.data_transformers.disable_max_rows()


# This notebook contains the graphs needed to expore the current staff shortage based on relevant online job adverts.
#
# ## 0. Load and clean relevant data

# ### 0.1 Load relevant datasets
# Load:
# - early years practitioner data;
# - similar job adverts data;
#
# Concatenate:
# - both datasets in order to clean the data

# In[7]:


# 0.1 Load datasets

# First, load in Early Year Practitioner (EYP) and similar job adverts
eyp_jobs = od.get_eyp_relevant_enriched_job_adverts()
sim_jobs = od.get_similar_enriched_job_adverts()

# concatenate the two dataframes then clean up the data
all_jobs = pd.concat([eyp_jobs, sim_jobs])

# Load in skills data
eyp_skills = od.get_eyp_relevant_skills()
sim_skills = od.get_similar_skills()


# ### 0.2 Clean up the datasets
# Clean up all job adverts by:
# - dropping duplicates as defined by the job advert being posted on the same day, with the same title in the same location;
# - dropping adverts posted before april 2021;
# - creating a series of time variables to allow for time series analysis;
# - drop adverts that are not in England;
# - drop columns that are not needed for analysis;
# - accounting for inflation by converting all salaries to March 2023 prices;
# - splitting cleaned data into two additional dataframes: one for clean EYP adverts and one for clean similar adverts.

# In[10]:


# 0.2 Clean up the dataset

sectors_to_include = [
    "Early Years Practitioner",
    "Primary School Teacher",
    "Secondary School Teacher",
    "Retail Assistant",
    "Waiter",
]

all_jobs_clean = (
    all_jobs.drop_duplicates(subset=["location", "job_title_raw", "created"])
    # make sure its after april given feedback
    .query("created >= '2021-04-01'")
    # Create a series of time variables to help with analysis
    .assign(created=lambda x: pd.to_datetime(x["created"]))
    .assign(year=lambda x: x["created"].dt.year.astype(str))
    .assign(month_year=lambda x: x["created"].dt.to_period("M"))
    .assign(month_year=lambda x: pd.to_datetime(x["month_year"].astype(str)))
    # flag whether in england or not
    .assign(england_geo=lambda x: x["itl_3_code"].apply(oau.is_england_geo))
    .query("england_geo == True")
    .query("sector in @sectors_to_include")
    .drop(columns=["is_large_geo", "england_geo"])
)

# accomodate for inflation
sal_cols = "min", "max"
for col in sal_cols:
    col_name = f"{col}_annualised_salary"
    inflation_col_name = f"inflation_adj_{col}_salary"
    all_jobs_clean[inflation_col_name] = all_jobs_clean.apply(
        lambda x: oau.calculate_inflation_adjusted_salary(
            original_salary=x[col_name], original_year=x.year
        ),
        axis=1,
    )
    all_jobs_clean[inflation_col_name] = all_jobs_clean[inflation_col_name].astype(
        float
    )
#

# Now that we have cleaned up the data, we can re-split it into EYP and similar jobs
# for future analysis
eyp_jobs_clean = (
    all_jobs_clean.query("sector == 'Early Years Practitioner'")
    # make sure qualifications are only up to level 6
    .query("qualification_level <= '6' | qualification_level.isna()").reset_index(
        drop=True
    )
)

sim_jobs_clean = all_jobs_clean.query(
    "sector != 'Early Years Practitioner'"
).reset_index(drop=True)

# Clean up skills data
id_2_sector_mapper = all_jobs_clean.set_index("id").sector.to_dict()
all_skills = pd.concat([eyp_skills, sim_skills])
all_skills["sector"] = all_skills.id.map(id_2_sector_mapper)
all_skills.dropna(subset=["sector"], inplace=True)


# ## 1. Generate tables for report
#
# Print figures to state in the report and save tables to s3 that are relevant for the report.

# In[16]:


# Print figures to include in the report

print(
    f"there are {len(eyp_jobs_clean)} job adverts in the EYP sector in England between {eyp_jobs_clean.created.min()} and {eyp_jobs_clean.created.max()}"
)
print("")
print(
    f"the minimum annualised salary range is between £{eyp_jobs_clean.inflation_adj_min_salary.min()} and £{eyp_jobs_clean.inflation_adj_min_salary.max()}"
)
print("")
print(
    f"the median minimum annualised salary is  £{eyp_jobs_clean.inflation_adj_min_salary.median()}"
)
print("")
print(
    f"the median maximum annualised salary is  £{eyp_jobs_clean.inflation_adj_max_salary.median()}"
)
print("")
print(
    f"the maximum annualised salary range is between £{eyp_jobs_clean.inflation_adj_max_salary.min()} and £{eyp_jobs_clean.inflation_adj_max_salary.max()}"
)
print("")
low_sal = eyp_jobs_clean[eyp_jobs_clean.inflation_adj_max_salary < 15000]
print(
    f"{len(low_sal)} or {round((len(low_sal)/len(eyp_jobs_clean))*100, 2)}% of jobs pay less than £15,000 per year"
)


# In[20]:


# Print the number of jobs by 'sector'
for sector, sector_info in all_jobs_clean.groupby("sector"):
    print(f"there are {len(sector_info)} jobs in the {sector} sector")


# In[21]:


# Median annualised salary and advertisement count per sector table

median_salary_sector = (
    all_jobs_clean.groupby("sector")
    .agg(
        {
            "id": "count",
            "inflation_adj_min_salary": "median",
            "inflation_adj_max_salary": "median",
        }
    )
    .reset_index()
    .rename(
        columns={
            "inflation_adj_min_salary": "Median Minimum Annualised Salary (£, March 2023 prices)",
            "inflation_adj_max_salary": "Median Maximum Annualised Salary (£, March 2023 prices)",
            "id": "Number of Job Adverts",
            "sector": "Sector",
        }
    )
)

# save table to s3
median_salary_sector = median_salary_sector.astype(
    {
        "Median Minimum Annualised Salary (£, March 2023 prices)": "int",
        "Median Maximum Annualised Salary (£, March 2023 prices)": "int",
    }
)

median_salary_sector_path = os.path.join(
    oau.output_table_path, "median_salary_count_per_sector.csv"
)
save_to_s3(BUCKET_NAME, median_salary_sector, median_salary_sector_path)


# In[22]:


# Median annualised salary per qualification level table

median_salary_qualification = (
    eyp_jobs_clean.groupby("qualification_level")
    .agg(
        {
            "id": "count",
            "inflation_adj_min_salary": "median",
            "inflation_adj_max_salary": "median",
        }
    )
    .reset_index()
    .rename(
        columns={
            "inflation_adj_min_salary": "Median Minimum Annualised Salary (£, March 2023 prices)",
            "inflation_adj_max_salary": "Median Maximum Annualised Salary (£, March 2023 prices)",
            "id": "Number of Job Adverts",
            "sector": "Sector",
        }
    )
)

median_salary_qualification_path = os.path.join(
    oau.output_table_path, "median_salary_count_per_qualification_level.csv"
)
save_to_s3(BUCKET_NAME, median_salary_qualification, median_salary_qualification_path)


# In[23]:


# Top 10 most common job titles per sector table

top_10_titles = (
    all_jobs_clean.groupby("sector")
    .agg({"job_title_raw": "value_counts"})
    .rename(columns={"job_title_raw": "count"})
    .groupby(level=0)
    .head(10)
    .reset_index()
    .rename(columns={"job_title_raw": "job_title"})
)

top_10_titles_path = os.path.join(oau.output_table_path, "top_10_titles_per_sector.csv")
save_to_s3(BUCKET_NAME, top_10_titles, top_10_titles_path)


# ## 2. Generate Graphs
#
# ### 2.1 Graphs related to salaries
#
# Generate:
# - boxplots by qualification level and sector
# - salary timeseries by sector with median England wage line

# In[24]:


# Generate Salary Distribution by Qualification Level Graph

box_plot_df = eyp_jobs_clean.query('itl_3_name != "London"').melt(
    id_vars=["sector", "qualification_level"],
    value_vars=["inflation_adj_min_salary", "inflation_adj_max_salary"],
    var_name="salary_type",
    value_name="salary",
)
box_plot_df = box_plot_df[box_plot_df["qualification_level"].isin(["2", "3", "6"])]
box_plot_df = (
    box_plot_df.dropna(subset=["qualification_level"])
    .assign(salary_type=lambda x: x.salary_type.replace(oau.salary_mapper))
    .rename(columns={"qualification_level": "Qualification Level"})
)

box_plot_graph = oau.generate_boxplot(box_plot_df, "Qualification Level", 0)

sal_qual_boxplot = au.configure_plots(
    box_plot_graph,
    chart_title="Annualised Salary Distribution by EYP Qualification Level",
    chart_subtitle=["Salaries are much higher for those with a degree (level 6)."],
)

sal_qual_boxplot


# In[25]:


# Generate Wage Ratio Graph

eyp_jobs_clean_no_london = eyp_jobs_clean.query('itl_3_name != "London"')
eyp_jobs_clean["min_wage_ratio"] = (
    eyp_jobs_clean["inflation_adj_min_salary"]
    / eyp_jobs_clean.inflation_adj_min_salary.median()
)
eyp_jobs_clean["max_wage_ratio"] = (
    eyp_jobs_clean["inflation_adj_max_salary"]
    / eyp_jobs_clean.inflation_adj_max_salary.median()
)

wage_ratio_df = (
    eyp_jobs_clean.groupby("qualification_level")
    .agg({"id": "count", "min_wage_ratio": "median", "max_wage_ratio": "median"})
    .reset_index()
    .rename(columns={"id": "count"})
)
wage_ratio_df["requires_degree"] = wage_ratio_df.qualification_level.apply(
    lambda x: True if x == "6" else False
)
wage_ratio_df = wage_ratio_df[wage_ratio_df.qualification_level.isin(["2", "3", "6"])]

qual_sorted = ["6", "3", "2"]

qual_count = (
    alt.Chart(wage_ratio_df)
    .mark_bar()
    .encode(
        # sort values by count in descending order
        x=alt.X("count:Q", title="# of Job Adverts"),
        y=alt.Y(
            "qualification_level",
            title="Qualification Level",
            sort=qual_sorted,
            type="ordinal",
        ),
        color=alt.Color("requires_degree", title="Requires Degree"),
    )
)

qual_wage_ratio = (
    alt.Chart(wage_ratio_df)
    .mark_bar()
    .encode(
        # sort values by count in descending order
        x=alt.X("max_wage_ratio:Q", title="Wage Ratio"),
        y=alt.Y(
            "qualification_level",
            title="Qualification Level",
            sort=qual_sorted,
            axis=None,
            type="ordinal",
        ),
        color=alt.Color("requires_degree", title="Requires Degree"),
    )
)

wage_line = alt.Chart(pd.DataFrame({"x": [1]})).mark_rule(color="red").encode(x="x")
qual_wage_ratio_line = qual_wage_ratio + wage_line

qual_graph = qual_count | qual_wage_ratio_line

au.configure_plots(
    qual_graph,
    chart_title="Wage Ratio by Qualification Level",
    chart_subtitle=[
        "The wage ratio is the ratio of the median salary for a given qualification",
        "level to the median salary for all EYP job adverts.",
    ],
)


# In[32]:


# Generate Salary Distribution by Sector Graph

box_plot_df = all_jobs_clean.query('itl_3_name != "London"').melt(
    id_vars=["sector"],
    value_vars=["inflation_adj_min_salary", "inflation_adj_max_salary"],
    var_name="salary_type",
    value_name="salary",
)
box_plot_df = box_plot_df.assign(
    salary_type=lambda x: x.salary_type.replace(oau.salary_mapper)
).rename(columns={"sector": "Sector"})

box_plot_graph = oau.generate_boxplot(box_plot_df, "Sector", 5)

sal_sect_boxplot = au.configure_plots(
    box_plot_graph,
    chart_title="Annualised Salary Distribution by Sector",
    chart_subtitle=[
        "The spread in salary varies by sector. Salaries appear most consistant in",
        "early years, retail, waiting and teaching assistant roles.",
    ],
)

sal_sect_boxplot


# In[33]:


# Generate Median Salary by Sector Over Time Graph
monthly_sector_sal_count = (
    all_jobs_clean.groupby(["sector", "month_year"])
    .agg({"inflation_adj_min_salary": "median", "inflation_adj_max_salary": "median"})
    .reset_index()
    .rename(
        columns={
            "inflation_adj_min_salary": "Median Minimum Annualised Salary (£)",
            "inflation_adj_max_salary": "Median Maximum Annualised Salary (£)",
        }
    )
)

monthly_sector_sal_count_melt = monthly_sector_sal_count.melt(
    id_vars=["sector", "month_year"],
    value_vars=[
        "Median Minimum Annualised Salary (£)",
        "Median Maximum Annualised Salary (£)",
    ],
    var_name="salary_type",
    value_name="salary",
)
monthly_sector_sal_count_melt.salary_type = (
    monthly_sector_sal_count_melt.salary_type.map(oau.clean_salary_mapper)
)
# Current 2023 median salary for England
monthly_sector_sal_count_melt["median_salary"] = 29588

salary_ts = (
    alt.Chart(monthly_sector_sal_count_melt)
    .mark_line()
    .encode(
        x=alt.X("month_year:T", title="Date"),
        y=alt.Y("salary", title="Median Annualised Salary (£)"),
        color=alt.Color("salary_type", title="Salary Type"),
    )
)

# add pattern to median line
median_wage_line = (
    alt.Chart(monthly_sector_sal_count_melt)
    .mark_rule(color="black", strokeDash=[1, 1])
    .encode(y="median_salary")
)

median_wage_sector = alt.layer(
    salary_ts, median_wage_line, data=monthly_sector_sal_count_melt
).facet("sector", columns=5)

sal_sect_ts = au.configure_plots(
    median_wage_sector,
    chart_title="Median Annualised Salary by Sector Over Time",
    chart_subtitle=[
        "Salaries for Early Year Practitioners, Retail Assistants and Waiters have",
        "almost consistently been below England's annual median salary since 2021.",
    ],
)

sal_sect_ts


# ### 2.2 Graphs related to count
#
# Generate:
# - monthly rolling average timeseries by sector

# In[34]:


# Generate Rolling Average of Job Adverts Over time by Sector Graph

sector_count_created = (
    all_jobs_clean.groupby(["sector", "created"]).size().reset_index(name="count")
)

sector_count_created["created"] = pd.to_datetime(sector_count_created["created"])
# Set 'created' as the index to use resample
sector_count_created.set_index("created", inplace=True)

# Calculate rolling mean per sector
monthly_mean = (
    sector_count_created.groupby("sector")["count"].rolling("30D").mean().reset_index()
)

rolling_avg_sector = (
    alt.Chart(monthly_mean)
    .mark_line()
    .encode(
        x=alt.X("created:T", title="Date"),
        y=alt.Y(f"count:Q", title="Rolling Monthly Average"),
        color=alt.Color("sector", legend=None),
    )
).facet("sector", columns=5, title=None)

sect_count_ts = au.configure_plots(
    rolling_avg_sector,
    chart_title="Rolling Monthly Average # of Job Adverts per Sector",
    chart_subtitle=[
        "There appears to be consistant demand across most sectors over time with the",
        "exception of Teaching Assistants. This may be due to the seasonality of the role.",
    ],
)

sect_count_ts


# ### 2.3 Graphs related to skills
#
# Generate:
# - a graph on sectors with the most similar skill profile
# - top skills at skill level for each sector

# In[37]:


# Calculate and get skill similarity scores based on the cosine similarity of
# skill count vectors between EYP and other sectors
sector_sim = oau.get_skill_similarity_scores(all_skills)

skill_profile_sims = (
    pd.DataFrame(sector_sim)
    .sort_values(by="skill_profile_similarity", ascending=False)
    .reset_index(drop=True)
)
skill_profile_sims[
    "skill_profile_similarity"
] = skill_profile_sims.skill_profile_similarity.apply(lambda x: round(x[0][0], 2))


# In[36]:


# Generate Similarity of EYP Skill Profile to Other Sectors Graph

most_similar_color = Color("red")
least_similar_color = Color("green")
similarity_colors = {
    sim_value / 10: str(c.hex)
    for sim_value, c in enumerate(
        list(most_similar_color.range_to(least_similar_color, 10))
    )
}

similar_sectors_text = pd.DataFrame(
    {"x": [0] * 4 + [1] * 4, "y": list(range(4, 0, -1)) + list(range(4, 0, -1))}
)
similar_sectors_text = similar_sectors_text[:-1]
similar_sectors_text_sim = pd.merge(
    skill_profile_sims, similar_sectors_text, left_index=True, right_index=True
)

similar_sectors_text_sim["skill_profile_similarity_approx"] = round(
    similar_sectors_text_sim.skill_profile_similarity, 1
)
similar_sectors_text_sim[
    "color"
] = similar_sectors_text_sim.skill_profile_similarity_approx.map(similarity_colors)

base = (
    alt.Chart(similar_sectors_text_sim)
    .mark_circle()
    .encode(
        x=alt.X("x", axis=None),
        y=alt.Y("y", axis=None),
        size=alt.SizeValue(400),
        color=alt.Color("color", title="Similarity", scale=None),
    )
)

text = base.mark_text(
    align="left", dx=15, dy=0, font="Century Gothic", fontWeight="bold", yOffset=-5
).encode(
    x="x",
    y="y",
    # alt size value of 10
    size=alt.SizeValue(14),
    text="sector",
)

score = base.mark_text(
    align="left", dx=15, dy=0, font="Century Gothic", fontStyle="italic", yOffset=10
).encode(
    x="x",
    y="y",
    # alt size value of 10
    size=alt.SizeValue(12),
    text="skill_profile_similarity",
)

similar_sectors_colors = pd.DataFrame(
    {
        "x": [0, 0, 0, 0],
        "y": [0, 0, 0, 0],
        "color": ["#008000", "#72aa00", "#d58e00", "#f00"],
        "Skill Profile Similarity": [
            "Very similar",
            "Quite similar",
            "Somewhat similar",
            "Not similar",
        ],
    }
)

legend_chart = (
    alt.Chart(similar_sectors_colors)
    .mark_circle(size=0)
    .encode(
        x=alt.X("x", title="", axis=None),
        y=alt.Y("y", title="", axis=None),
        color=alt.Color(
            "Skill Profile Similarity",
            scale=alt.Scale(
                domain=list(
                    dict(
                        zip(
                            similar_sectors_colors["Skill Profile Similarity"],
                            similar_sectors_colors["color"],
                        )
                    ).keys()
                ),
                range=list(
                    dict(
                        zip(
                            similar_sectors_colors["Skill Profile Similarity"],
                            similar_sectors_colors["color"],
                        )
                    ).values()
                ),
            ),
            legend=alt.Legend(title=""),
        ),
    )
).properties(width=-1, height=100)

base_text = base + text + score
base_text_legend = base_text | legend_chart

sim_skills = au.configure_plots(
    base_text_legend,
    chart_title="Similarity of EYP Skill Profile to Other Sectors",
    chart_subtitle=[""],
)
sim_skills


# In[43]:


# Generate top skills at the skill level for all jobs

skill2name_mapper = all_skills.set_index("esco_id").esco_label.to_dict()
all_skills_count = (
    all_skills.query("esco_id.str.len() > 10")
    .groupby(["sector", "esco_id"])
    .size()
    .reset_index()
    .rename(columns={0: "count"})
)

top_skills_per_sector = []
for sector, sector_info in all_skills_count.groupby("sector"):
    sector_info["job_ad_percent"] = (
        sector_info["count"] / all_skills.query(f"sector == '{sector}'").id.nunique()
    ) * 100
    top_skills_count = sector_info.sort_values(
        by="job_ad_percent", ascending=False
    ).head(10)
    top_skills_per_sector.append(top_skills_count)
top_skills_per_sector_df = pd.concat(top_skills_per_sector).reset_index(drop=True)
top_skills_per_sector_df["esco_label"] = top_skills_per_sector_df.esco_id.map(
    skill2name_mapper
)

# create flag for whether the skill is in the top 10 EYP skills for the sector
top_skills_per_sector_df["in_eyp_top_skills"] = top_skills_per_sector_df.apply(
    lambda x: True
    if x.esco_label
    in top_skills_per_sector_df[
        top_skills_per_sector_df.sector == "Early Years Practitioner"
    ].esco_label.unique()
    else False,
    axis=1,
)

charts = []
sectors = top_skills_per_sector_df.sector.unique()
for i in range(len(sectors)):
    bar_chart = (
        alt.Chart(
            top_skills_per_sector_df.query(f"sector == '{sectors[i]}'"),
            title=f"Top Skills for {sectors[i]}",
        )
        .mark_bar()
        .encode(
            y=alt.Y(
                "esco_label", title=None, sort=None, axis=alt.Axis(labelLimit=5000)
            ),
            x=alt.X("job_ad_percent", title="% of Job Adverts"),
            color=alt.Color(
                "in_eyp_top_skills",
                title="In EYP Top Skills?",
                scale=alt.Scale(
                    domain=[True, False],
                    range=[au.NESTA_COLOURS[0], au.NESTA_COLOURS[1]],
                ),
            ),
        )
    )
    charts.append(bar_chart)

top_skills_barchart = alt.vconcat(*charts[:3]) | alt.vconcat(*charts[3:])

au.configure_plots(
    top_skills_barchart,
    chart_title="Top Skills in Each Sector",
    chart_subtitle=[
        " ",
        "There are overlaps in the top skills across all sectors.",
        " ",
    ],
)

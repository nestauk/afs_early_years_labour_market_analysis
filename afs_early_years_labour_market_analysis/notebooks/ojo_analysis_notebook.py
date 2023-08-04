# %% [markdown]
# This notebook contains the analysis for current staff shortages in early years practitioner job adverts
# and similar sectors.
#
# Demand is proxied by:
# - percent change in # of job adverts;
# - change in salaries.

# %%
# import relevant libraries
import afs_early_years_labour_market_analysis.getters.ojd_daps as od
import afs_early_years_labour_market_analysis.analysis.analysis_utils as au
from afs_early_years_labour_market_analysis import BUCKET_NAME
from afs_early_years_labour_market_analysis.getters.data_getters import save_to_s3

import ojo_analysis_utils as oau

import pandas as pd
import altair as alt
import os
import ast
import geopandas as gpd

from colour import Color

# %%
# disable max rows for altair graphs
alt.data_transformers.disable_max_rows()

# %% [markdown]
# This notebook contains the graphs needed to expore the current staff shortage based on relevant online job adverts.
#
# ## 0. Load and clean relevant data

# %% [markdown]
# ### 0.1 Load relevant datasets
# Load:
# - early years practitioner data;
# - similar job adverts data;
#
# Concatenate:
# - both datasets in order to clean the data

# %%
# 0.1 Load datasets

# First, load in Early Year Practitioner (EYP) and similar job adverts
eyp_jobs = od.get_eyp_relevant_enriched_job_adverts()
sim_jobs = od.get_similar_enriched_job_adverts()

# concatenate the two dataframes then clean up the data
all_jobs = pd.concat([eyp_jobs, sim_jobs])

# Load in skills data
eyp_skills = od.get_eyp_relevant_skills()
sim_skills = od.get_similar_skills()

# %% [markdown]
# ### 0.2 Clean up the datasets
# Clean up all job adverts by:
# - dropping duplicates as defined by the job advert being posted on the same day, with the same title in the same location;
# - dropping adverts posted before april 2021;
# - creating a series of time variables to allow for time series analysis;
# - drop adverts that are not in England;
# - drop columns that are not needed for analysis;
# - accounting for inflation by converting all salaries to March 2023 prices;
# - splitting cleaned data into two additional dataframes: one for clean EYP adverts and one for clean similar adverts.

# %%
# 0.2 Clean up the dataset

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
    .assign(
        qualification_level=lambda x: x["qualification_level"].replace(oau.qual_mapper)
    )
    .reset_index(drop=True)
)
sim_jobs_clean = all_jobs_clean.query(
    "sector != 'Early Years Practitioner'"
).reset_index(drop=True)

# Clean up skills data
id_2_sector_mapper = all_jobs_clean.set_index("id").sector.to_dict()
all_skills = pd.concat([eyp_skills, sim_skills])
all_skills["sector"] = all_skills.id.map(id_2_sector_mapper)
all_skills.dropna(subset=["sector"], inplace=True)

# %% [markdown]
# ## 1. Generate tables for report
#
# Print figures to state in the report and save tables to s3 that are relevant for the report.

# %%
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

# %%
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

# %%
# Median annualised salary per qualification level table

median_salary_qualification = (
    eyp_jobs_clean.groupby("qualification_level")
    .agg(
        {
            "id": "count",
            "inflation_adj_min_salary": "mean",
            "inflation_adj_max_salary": "mean",
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

# %%
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

# %%
# Percent difference in the # of job adverts per sector table
# (Per laura's request)

early_df = all_jobs_clean.query(
    "created.between(@oau.early_date_range[0], @oau.early_date_range[1])"
)
late_df = all_jobs_clean.query(
    "created.between(@oau.late_date_range[0], @oau.late_date_range[1])"
)

sector_count = pd.merge(
    early_df.groupby("sector")
    .size()
    .reset_index(name="count")
    .sort_values(by="count", ascending=False),
    late_df.groupby("sector")
    .size()
    .reset_index(name="count")
    .sort_values(by="count", ascending=False),
    on="sector",
    suffixes=("_early", "_late"),
)

sector_percent_change = sector_count.assign(
    difference=lambda x: x.count_late - x.count_early
).assign(percent_change=lambda x: (x.difference / x.count_early) * 100)

sector_percent_change_path = os.path.join(
    oau.output_table_path, "percent_change_sector.csv"
)
save_to_s3(BUCKET_NAME, sector_percent_change, sector_percent_change_path)

# %% [markdown]
# ## 2. Generate Graphs
#
# ### 2.1 Graphs related to salaries
#
# Generate:
# - boxplots by qualification level and sector
# - salary timeseries by sector with median England wage line

# %%
# Generate Salary Distribution by Qualification Level Graph

box_plot_df = eyp_jobs_clean.query('itl_3_name != "London"').melt(
    id_vars=["sector", "qualification_level"],
    value_vars=["inflation_adj_min_salary", "inflation_adj_max_salary"],
    var_name="salary_type",
    value_name="salary",
)
box_plot_df = (
    box_plot_df.dropna(subset=["qualification_level"])
    .assign(salary_type=lambda x: x.salary_type.replace(oau.salary_mapper))
    .rename(columns={"qualification_level": "Qualification Level"})
)

box_plot_df = oau.generate_boxplot(box_plot_df, "Qualification Level", 0)

sal_qual_boxplot = au.configure_plots(
    box_plot_df,
    chart_title="Annualised Salary Distribution by EYP Qualification Level",
    chart_subtitle=["The spread in salary across qualification levels is quite large."],
)

# %%
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

box_plot_df = oau.generate_boxplot(box_plot_df, "Sector", 4)

sal_sect_boxplot = au.configure_plots(
    box_plot_df,
    chart_title="Annualised Salary Distribution by Sector",
    chart_subtitle=[
        "The spread in salary varies by sector. Salaries appear most consistant in",
        "early years, retail, waiting and teaching assistant roles.",
    ],
)

# %%
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
).facet("sector", columns=4)

sal_sect_ts = au.configure_plots(
    median_wage_sector,
    chart_title="Median Annualised Salary by Sector Over Time",
    chart_subtitle=[
        "Salaries for Early Year Practitioners, Retail Assistants and Waiters have",
        "almost consistently been below England's annual median salary since 2021.",
    ],
)

# %% [markdown]
# ### 2.2 Graphs related to count
#
# Generate:
# - monthly rolling average timeseries by sector

# %%
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
).facet("sector", columns=4, title=None)

sect_count_ts = au.configure_plots(
    rolling_avg_sector,
    chart_title="Rolling Monthly Average # of Job Adverts per Sector",
    chart_subtitle=[
        "There appears to be consistant demand across most sectors over time with the",
        "exception of Teaching Assistants. This may be due to the seasonality of the role.",
    ],
)

# %% [markdown]
# ### 2.3 Graphs related to percent change in demand
#
# Generate:
# - Percent change in demand by location (chart)
# - Percent change in demand by location and sector (map)

# %%
# Create a dataframe with the number of job adverts per month per sector and percent change from early to late period

early_df_percent = (
    early_df.groupby(["itl_3_name", "sector"])
    .size()
    # make sure size is above 10
    .reset_index(name="count")
    .query("count > 10")
)

late_df_percent = (
    late_df.groupby(["itl_3_name", "sector"])
    .size()
    # make sure size is above 10
    .reset_index(name="count")
    .query("count > 10")
)

percent_change_df = pd.merge(
    early_df_percent,
    late_df_percent,
    on=["itl_3_name", "sector"],
    suffixes=("_early", "_late"),
)
percent_change_df = (
    percent_change_df.assign(difference=lambda x: abs(x.count_early - x.count_late))
    .assign(percent_change=lambda x: abs((x.difference / x.count_early) * 100))
    .assign(decline=lambda x: x.count_early > x.count_late)
)

# %%
# Top 10 Locations By Percent Change in # of Job Adverts for EYP Graph

colors = [au.NESTA_COLOURS[0], au.NESTA_COLOURS[1]]

loc_changes = (
    alt.Chart(
        percent_change_df.query('sector == "Early Years Practitioner"')
        .sort_values(by="percent_change", ascending=False)
        .head(10)
    )
    .mark_circle()
    .encode(
        x=alt.X("percent_change", title="# of Job Adverts Percent Change (%)"),
        y=alt.Y(
            "itl_3_name", title="Location", sort=None, axis=alt.Axis(labelLimit=5000)
        ),
        size=alt.Size(
            "difference",
            title=["Difference in", "Job Advert #"],
        ),
        color=alt.Color("decline", title="Decline", scale=alt.Scale(range=colors)),
    )
    .properties(width=500, height=300)
)
loc_changes = au.configure_plots(
    loc_changes,
    chart_title="The Top Locations with the Largest Percentage Change in EYP Job Adverts",
    chart_subtitle=[
        "The size of the circle represents the difference in the number of job adverts between 2021-2022",
        "while the color indicates whether the percent change is a decline or not.",
    ],
)

eyp_pc_top_10 = loc_changes.configure_legend(
    labelLimit=500,
)

# %%
# Percent Change in Demand by Location and Sector (map)

nuts_geo_data = oau.get_nuts_geo_data()
percent_change_geo_df = pd.merge(
    percent_change_df, nuts_geo_data, left_on="itl_3_name", right_on="NUTS_NAME"
)

percent_change_geo_df.loc[
    percent_change_geo_df[percent_change_geo_df.decline == True].index, "percent_change"
] = (percent_change_geo_df.percent_change * -1)
percent_change_geo_df = gpd.GeoDataFrame(percent_change_geo_df, geometry="geometry")

domain = [
    percent_change_geo_df.percent_change.min(),
    0,
    percent_change_geo_df.percent_change.max(),
]


map_percent_change = (
    alt.Chart(percent_change_geo_df.query('sector == "Early Years Practitioner"'))
    .mark_geoshape(stroke="white", color="black")
    .encode(
        color=alt.Color(
            "percent_change:Q",
            scale=alt.Scale(domain=domain, range=oau.map_color_range),
            title="Percent Change (%)",
            legend=alt.Legend(orient="bottom", direction="horizontal"),
        )
    )
)

# change size of map and add title/subtitle
map_percent_change = au.configure_plots(
    map_percent_change,
    chart_title="Percent Change in EYP Demand",
    chart_subtitle=[
        "Demand is defined as the percent change in the number of online job postings",
        "at two time points. Regions in white indicate not enough data to report.",
    ],
).properties(width=500, height=300)

# %% [markdown]
# ### 2.4 Graphs related to skills
#
# Generate:
# - a graph on sectors with the most similar skill profile

# %%
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

# %%
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

"""
Script to filter enriched datasets to use for OJO analysis

python -m afs_early_years_labour_market_analysis.pipeline.data_enrichment.filter_relevant_jobs
"""
from afs_early_years_labour_market_analysis import PROJECT_DIR
from afs_early_years_labour_market_analysis.getters.ojd_daps import (
    get_eyp_relevant_enriched_job_adverts,
    get_similar_enriched_job_adverts,
)

import pandas as pd
import os
from typing import Optional

sim_jobs_occ_dict = {
    "Primary School": "Primary School Teacher",
    "Special Needs": "Special Needs Teacher",
    "Secondary School": "Secondary School Teacher",
}

analysis_data_path = (
    PROJECT_DIR / "afs_early_years_labour_market_analysis/analysis/data"
)

if not os.path.exists(analysis_data_path):
    os.makedirs(analysis_data_path)


def filter_data(all_jobs: pd.DataFrame, col_to_filter: str) -> pd.DataFrame:
    """Filter data by sector name and column to filter on.

    Args:
        col_to_filter (str): Column to filter data by

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    return (
        all_jobs.groupby(["month_year", col_to_filter])
        .agg(
            {
                "id": "count",
                "max_annualised_salary": "median",
                "min_annualised_salary": "median",
            }
        )
        .reset_index()
        .rename(
            columns={
                "id": "count",
                "max_annualised_salary": "median_max_annualised_salary",
                "min_annualised_salary": "median_min_annualised_salary",
            }
        )
        .query('month_year >= "2020-12"')
    )


if __name__ == "__main__":
    # change this to load data from repo
    print("loading eyp jobs...")
    eyp_jobs = (
        get_eyp_relevant_enriched_job_adverts()
        .assign(sector="Early Years Practitioner")
        .query("qualification_level < 7")
        .reset_index(drop=True)
    )

    print("loading sim jobs...")
    sim_jobs = (
        get_similar_enriched_job_adverts()
        # relace sector names
        .assign(sector=lambda x: x["sector"].replace(sim_jobs_occ_dict))
        # keep top 5 sectors
        .query(
            f'sector.isin({list(sim_jobs_occ_dict.values()) + ["Sales Assistant", "Teaching Assistant"]})'
        )
    )

    print("concatenating jobs...")
    all_jobs = (
        pd.concat([eyp_jobs, sim_jobs])
        .assign(created=lambda x: pd.to_datetime(x["created"]))
        .assign(month_year=lambda x: x.created.dt.strftime("%Y-%m"))
    )

    # clean up itl 3 names to make sure all of London is merged
    print("cleaning up itl names...")
    itl_3_london = (
        all_jobs.query("~itl_2_name.isna()")
        .query('itl_2_name.str.contains("London")')
        .itl_3_name.to_list()
    )
    all_jobs.itl_3_name.replace(
        dict(zip(itl_3_london, ["London" for _ in range(len(itl_3_london))])),
        inplace=True,
    )

    # for the demand and salary over time by sector
    print("saving salary and count over time by similar sector...")
    sal_demand = filter_data(all_jobs, col_to_filter="sector")
    sal_demand.to_csv(
        analysis_data_path / "sector_sal_count_over_time.csv", index=False
    )

    # for differences in salary and count by qualification level
    print("saving median salary and count by qualification level...")
    qual_demand = filter_data(all_jobs, col_to_filter="qualification_level")
    qual_demand.to_csv(analysis_data_path / "qual_sal_count_over_time.csv", index=False)

    print("saving salary by qualification level...")
    sal_by_level = (
        all_jobs.query("~qualification_level.isna()")[
            ["qualification_level", "max_annualised_salary"]
        ]
        .pivot(columns="qualification_level", values="max_annualised_salary")
        .dropna(how="all")
    )

    sal_by_level.to_csv(analysis_data_path / "sal_by_qual_level.csv", index=False)

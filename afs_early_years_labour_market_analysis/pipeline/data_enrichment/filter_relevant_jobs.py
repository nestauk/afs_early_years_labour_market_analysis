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
import json

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

    print("save eyp job metadata...")
    eyp_jobs_metadata = {}
    eyp_jobs_clean = all_jobs.query(
        'sector == "Early Years Practitioner"'
    ).drop_duplicates(subset=["id"])

    eyp_jobs_metadata["no_jobs"] = len(eyp_jobs_clean)
    eyp_jobs_metadata[
        "min_sal_info"
    ] = eyp_jobs_clean.min_annualised_salary.describe().to_dict()
    eyp_jobs_metadata[
        "max_sal_info"
    ] = eyp_jobs_clean.max_annualised_salary.describe().to_dict()
    eyp_jobs_metadata["job_adverts_range"] = (
        str(eyp_jobs_clean.created.min().date()),
        str(eyp_jobs_clean.created.max().date()),
    )

    with open(analysis_data_path / "eyp_job_metadata.json", "w") as fp:
        json.dump(eyp_jobs_metadata, fp)

    print("saving qualification level data...")
    source = (
        eyp_jobs_clean.groupby("qualification_level")
        .agg({"max_annualised_salary": "median", "id": "count"})
        .reset_index()
        .assign(
            qualification_level=lambda x: x.qualification_level.astype(int).astype(str)
        )
        .assign(
            wage_ratio=lambda x: x.max_annualised_salary
            / eyp_jobs.max_annualised_salary.median()
        )
        .rename(
            columns={
                "id": "count",
                "max_annualised_salary": "median_salary",
                "qualification_level": "Qualification Level",
            }
        )
    )
    source["degree_not"] = source["Qualification Level"].apply(
        lambda x: "Yes" if x >= "4" else "No"
    )
    source.to_csv(analysis_data_path / "qual_data.csv", index=False)

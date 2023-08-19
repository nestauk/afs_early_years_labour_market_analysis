"""
Script to clean up enriched datasets to use for OJO analysis and streamlit app.

This script:
    - drops sectors that are no longer relevant;
    - accomodates for inflation;
    - drops duplicates;
    - drops jobs that were created before April 2021;
    - cleans up skills data.

python -m afs_early_years_labour_market_analysis.pipeline.data_enrichment.clean_relevant_jobs
"""
# import relevant libraries
import pandas as pd
import afs_early_years_labour_market_analysis.getters.ojd_daps as od
from afs_early_years_labour_market_analysis import PROJECT_DIR
import re
from typing import Dict, Union

# Inflation rates for 2021, 2022 and 2023 (avg. monthly inflation rate from jan 2023 to march 2023)
# obtained from the OECD - https://data.oecd.org/price/inflation-cpi.htm (accessed 20/04/2021)
inflation_rate_dict = {"2020": 0.01, "2021": 0.025, "2022": 0.079, "2023": 0.0896667}


def itl_to_nation(itl_1_code: str) -> str:
    """
    Args:
        itl_1_code (str): itl 1 code

    Returns:
        str: nation name
    """

    england_codes = "|".join(
        ["TLK", "TLJ", "TLI", "TLE", "TLD", "TLG", "TLH", "TLC", "TLF"]
    )
    england_codes_regex = f"^({england_codes})"

    wales_code, scotland_code, ni_code = "TLL", "TLM", "TLN"

    if isinstance(itl_1_code, str) == False:
        return None

    matches = re.match(england_codes_regex, itl_1_code)
    if matches:
        return "England"
    elif itl_1_code == wales_code:
        return "Wales"
    elif itl_1_code == scotland_code:
        return "Scotland"
    elif itl_1_code == ni_code:
        return "Northern Ireland"


def calculate_inflation_adjusted_salary(
    original_salary: int,
    original_year: int,
    inflation_rate_dict: Dict[int, float] = inflation_rate_dict,
) -> Union[int, None]:
    """Calculate the inflation adjusted salary for a given year

    Args:
        original_salary (int): Original salary
        original_year (int): Original year
        inflation_rate_dict (Dict[int, float], optional): Inflation data dictionary.
            Defaults to inflation_rate_dict.

    Returns:
        Union[int, None]: If original salary,
            return inflation adjusted salary, else return None.
    """
    if isinstance(original_year, str):
        original_year = int(original_year)

    if not isinstance(original_salary, float):
        original_salary = float(original_salary)

    inflation_data = {
        k: v for k, v in inflation_rate_dict.items() if int(k) > original_year
    }
    inflation_data = dict(sorted(inflation_data.items()))

    cumulative_inflation = 1.0
    for year in inflation_data.keys():
        cumulative_inflation *= 1 + inflation_data.get(year)

    if original_salary:
        inflated_salary = original_salary * cumulative_inflation

        return round(inflated_salary, 2)

    else:
        return None


if __name__ == "__main__":
    # change this to load data from repo

    # 0.1 Load datasets
    # First, load in Early Year Practitioner (EYP) and similar job adverts
    print("Loading relevant datasets...")
    eyp_jobs = od.get_eyp_relevant_enriched_job_adverts()
    sim_jobs = od.get_similar_enriched_job_adverts()

    # concatenate the two dataframes then clean up the data
    all_jobs = pd.concat([eyp_jobs, sim_jobs]).rename(columns={"sector": "profession"})

    # Load in skills data
    eyp_skills = od.get_eyp_relevant_skills()
    sim_skills = od.get_similar_skills()

    print("Cleaning dataset...")
    # 0.2 Clean up the dataset

    professions_to_include = [
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
        .assign(nation=lambda x: x["itl_1_code"].apply(itl_to_nation))
        .query("profession in @professions_to_include")
        .drop(columns=["is_large_geo"])
    )

    # accomodate for inflation
    sal_cols = "min", "max"
    for col in sal_cols:
        col_name = f"{col}_annualised_salary"
        inflation_col_name = f"inflation_adj_{col}_salary"
        all_jobs_clean[inflation_col_name] = all_jobs_clean.apply(
            lambda x: calculate_inflation_adjusted_salary(
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
    print("Splitting and saving cleaned datasets...")
    eyp_jobs_clean = (
        all_jobs_clean.query("profession == 'Early Years Practitioner'")
        # make sure qualifications are only up to level 6
        .query("qualification_level <= '6' | qualification_level.isna()").reset_index(
            drop=True
        )
    )
    eyp_jobs_clean.to_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/eyp_jobs_clean.csv",
        index=False,
    )

    sim_jobs_clean = all_jobs_clean.query(
        "profession != 'Early Years Practitioner'"
    ).reset_index(drop=True)
    sim_jobs_clean.to_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/sim_jobs_clean.csv",
        index=False,
    )

    # Clean up skills data
    all_jobs_clean["id"] = all_jobs_clean["id"].astype(int)
    id2profession = all_jobs_clean.set_index("id").profession.to_dict()
    id2nation = all_jobs_clean.set_index("id").nation.to_dict()
    all_skills = pd.concat([eyp_skills, sim_skills])

    all_skills = (
        all_skills.assign(id=lambda x: x["id"].astype(int))
        .assign(profession=lambda x: x["id"].map(id2profession))
        .assign(nation=lambda x: x["id"].map(id2nation))
        .dropna(subset=["profession"])
    )

    all_skills.to_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/all_skills_clean.csv",
        index=False,
    )

"""
This pipeline generates a custom dataset using various
    EYP sources to create a dataset for England that can be used to
    analyse the predicted labour shortfall in the early years
    sector.

python afs_early_years_labour_market_analysis/pipeline/data_collection/curate_eyp_data.py
"""
from afs_early_years_labour_market_analysis import BUCKET_NAME, logger
from afs_early_years_labour_market_analysis.getters.data_getters import (
    load_s3_data,
    save_to_s3,
)

import pandas as pd
import re
import numpy as np

RELEVANT_COLS = [
    "Local Authority",
    "LAD21CD",
    "all_eyr_providers",
    "all_eyr_places",
    "all_non_eyr_providers",
    "all_eyr_total_providers",
    "childminder_eyr_providers",
    "childminder_eyr_places",
    "childminder_non_eyr_ providers",
    "childminder_eyr_total_providers",
    "Age: Aged under 1 year; measures: Value",
    "Age: Aged 1 year; measures: Value",
    "Age: Aged 2 years; measures: Value",
    "Age: Aged 3 years; measures: Value",
    "Age: Aged 4 years; measures: Value",
    "Index of Multiple Deprivation (IMD) 2010",
    "Households in poverty",
    "Children aged 0-4 in poverty",
    "Urbanites",
    "Suburbanites",
    "Rural Residents",
]

LA_DATA_COLS_TO_CLEAN = [
    "LA Code",
    "Index of Multiple Deprivation (IMD) 2010",
    "Households in poverty",
    "Children aged 0-4 in poverty",
    "Urbanites",
    "Suburbanites",
    "Rural Residents",
]

RX = "[" + re.escape("".join([":", "£", "-", ","])) + "]"

# step 0. get the paths of datasets you will use to generate curated dataset

logger.info("loading relevant datasets from s3...")
# This contains the info on the # of children in each local authority
census = load_s3_data(BUCKET_NAME, "inputs/census/census2021-ts007-ltla.csv")

# Contains information on the # of early years childminder providers per local authority
registered_childminder_providers = pd.read_excel(
    f"s3://{BUCKET_NAME}/inputs/childcare_providers_inspections/num_providers.xlsx",
    sheet_name="childminder_eyr",
)
registered_all_eyr_providers = pd.read_excel(
    f"s3://{BUCKET_NAME}/inputs/childcare_providers_inspections/num_providers.xlsx",
    sheet_name="Sheet1",
)

# Contains information on levels of deprivation per local authority and rural_urban classification
la_data1 = pd.read_csv(
    f"s3://{BUCKET_NAME}/inputs/loc_metadata/local_insight_January_2023_LSOA_download_1.csv",
    low_memory=False,
)[5:][LA_DATA_COLS_TO_CLEAN].reset_index(drop=True)

# 30 hour awareness
new_prov_awareness = load_s3_data(
    BUCKET_NAME, "inputs/ceyps/ceysp21_awareness_30_hours.csv"
)[:25]

# % of children currently using EYP per local authority by age
currently_using_eyp = load_s3_data(
    BUCKET_NAME, "inputs/ceyps/ceysp21_childcare_use_0_4_ts.csv"
)

# Proportion of families with children aged 0-4 using any childcare
family_prop_any_childcare = load_s3_data(
    BUCKET_NAME, "inputs/ceyps/ceysp21_childcare_use_0_4_ts_families.csv"
)

# Proportion of families using childcare and paying for it themselves

# lsoa to las in England and Wales
lsoa_to_la = load_s3_data(
    BUCKET_NAME,
    "inputs/loc_metadata/LSOA_(2011)_to_LSOA_(2021)_to_Local_Authority_District_(2022)_Lookup_for_England_and_Wales_(Version_2).csv",
)
old_la_to_new_la = load_s3_data(
    BUCKET_NAME,
    "inputs/loc_metadata/Local_Authority_District_(2011)_to_Local_Authority_District_(2021)_Lookup_for_England_and_Wales.csv",
)
region_to_la = load_s3_data(
    BUCKET_NAME,
    "inputs/loc_metadata/Local_Authority_District_to_Region_(April_2021)_Lookup_in_England.csv",
)

# number of nurses/childminders per region
nurse_childminder_number = load_s3_data(
    BUCKET_NAME, "inputs/census/occ_pop_by_region.csv"
)

# turnover rates of staff by provider
staff_turnover_rate = load_s3_data(
    BUCKET_NAME, "inputs/ceyps/sceyp22_staff_turnover_rev.csv"
)

# children currently using free childcare (15-hours)

b_eyp_percent_registered = (
    load_s3_data(
        BUCKET_NAME,
        "inputs/education_provision/1b_early_years_provision_percentage_registered_2018_2022_corrected.csv",
    )
    .dropna(
        subset=["region_code", "region_name", "old_la_code", "new_la_code", "la_name"],
        how="all",
    )
    .reset_index(drop=True)
)

# average childcare setting size by provider type

if __name__ == "__main__":
    # Clean up and merge LA-level data (census information, # of early year practitioner places, LA metadata)
    logger.info("cleaning and merging LA-level data...")
    for col in LA_DATA_COLS_TO_CLEAN[1:]:
        # convert to numbers
        la_data1[col] = la_data1[col].astype(float)

    la_data1_clean = (
        la_data1.groupby("LA Code")
        .agg(
            {
                "Index of Multiple Deprivation (IMD) 2010": np.mean,
                "Households in poverty": np.mean,
                "Children aged 0-4 in poverty": np.mean,
                "Urbanites": np.mean,
                "Suburbanites": np.mean,
                "Rural Residents": np.mean,
            }
        )
        .reset_index(drop=False)
    )

    # merge datasets that are at the LA level
    registered_providers_la = pd.merge(
        registered_childminder_providers,
        old_la_to_new_la,
        left_on="Local Authority",
        right_on="LAD21NM",
    )
    all_registered_providers_la = pd.merge(
        registered_providers_la, registered_all_eyr_providers, on="Local Authority"
    )
    registered_providers_la_age = pd.merge(
        all_registered_providers_la,
        census,
        left_on="LAD21CD",
        right_on="geography code",
    )
    registered_providers_la_age_la_data = pd.merge(
        registered_providers_la_age,
        la_data1_clean,
        left_on="LAD21CD",
        right_on="LA Code",
        how="left",
    )

    la_data = registered_providers_la_age_la_data[RELEVANT_COLS].rename(
        columns={
            "Local Authority": "local_authority",
            "LAD21CD": "local_authority_code",
            "Age: Aged under 1 year; measures: Value": "age_under_1",
            "Age: Aged 1 year; measures: Value": "age_1",
            "Age: Aged 2 years; measures: Value": "age_2",
            "Age: Aged 3 years; measures: Value": "age_3",
            "Age: Aged 4 years; measures: Value": "age_4",
            "Index of Multiple Deprivation (IMD) 2010": "imd_2010",
            "Households in poverty": "households_in_poverty",
            "Children aged 0-4 in poverty": "children_0_4_in_poverty",
            "Urbanites": "urbanites",
            "Suburbanites": "suburbanites",
            "Rural Residents": "rural_residents",
        }
    )

    logger.info("add in national-level information...")

    new_prov_awareness_clean = new_prov_awareness.T[5:].reset_index(drop=True)
    new_prov_awareness_clean.columns = [
        f"{re.sub(RX, '', i).replace('  ', ' ').replace(' ', '_').lower()}_30_hours_aware"
        for i in new_prov_awareness_clean.iloc[0].to_list()
    ]
    new_prov_awareness_clean = new_prov_awareness_clean[2:].reset_index(drop=True)

    # add new prov awareness data
    la_data = la_data.assign(**new_prov_awareness_clean).fillna(method="ffill")
    currently_using_eyp_clean = currently_using_eyp[:3].transpose().reset_index()

    currently_using_eyp_clean.columns = [
        f"prop_children_in_{i.lower().replace(' ', '_')}"
        for i in currently_using_eyp_clean.iloc[5]
    ]

    currently_using_eyp_clean = (
        currently_using_eyp_clean.loc[[6]]
        .drop(columns=["prop_children_in_childcare_type"])
        .reset_index(drop=True)
    )

    la_data = la_data.assign(**currently_using_eyp_clean).fillna(method="ffill")

    family_prop_any_childcare_clean = (
        family_prop_any_childcare[:3].transpose().reset_index()
    )

    family_prop_any_childcare_clean.columns = [
        f"prop_families_using_{i.lower().replace(' ', '_')}"
        for i in family_prop_any_childcare_clean.iloc[5]
    ]

    family_prop_any_childcare_clean = (
        family_prop_any_childcare_clean.loc[[6]]
        .drop(columns=["prop_families_using_childcare_type"])
        .reset_index(drop=True)
    )

    la_data = la_data.assign(**family_prop_any_childcare_clean).fillna(method="ffill")

    logger.info(
        "add in uniform features i.e. childcare to staff ratios, staff turnover..."
    )

    # add uniform ratio of children to staff values - from
    la_data["under_2_staff_child_ratio"] = 1 / 3
    la_data["age_2_staff_child_ratio"] = 1 / 4
    la_data["3_over_staff_child_ratio_level_6_qualification"] = 1 / 13
    la_data["3_over_staff_child_ratio_no_level_6_qualification"] = 1 / 8
    la_data["age_3_staff_child_ratio_level_6_qualification_independent_school"] = 1 / 13
    la_data["age_3_staff_child_ratio_no_level_6_qualification_independent_school"] = (
        1 / 8
    )
    la_data["childminder_child_ratio"] = 1 / 6

    # add uniform turnover rates of staff by provider
    la_data["avg_staff_turnover_rate"] = 0.095

    # add uniform # of children per setting type as of 2021 - from Childcare and early years providers survey: 2021
    la_data["average_num_registered_places_private_groupbased"] = 53
    la_data["average_num_registered_places_voluntary_groupbased"] = 37
    la_data["average_num_registered_places_all_groupbased"] = 47
    la_data["average_num_registered_places_all_childminders"] = 6

    logger.info("add region-level information...")

    nurse_childminder_number.columns = (
        "region",
        "total",
        "conf",
        "region_num_primary_nursery_teacher",
        "region_primary_nursery_teacher_conf",
        "region_num_nursery_nurses_assistants",
        "region_num_nursery_nurses_assistants_conf",
        "region_num_childminders",
        "region_num_childminders_conf",
    )
    nurse_childminder_number = nurse_childminder_number.drop([0])

    nurse_childminder_number_la = pd.merge(
        nurse_childminder_number, region_to_la, left_on="region", right_on="RGN21NM"
    )[
        [
            "region",
            "RGN21CD",
            "LAD21CD",
            "region_num_primary_nursery_teacher",
            "region_primary_nursery_teacher_conf",
            "region_num_nursery_nurses_assistants",
            "region_num_nursery_nurses_assistants_conf",
            "region_num_childminders",
            "region_num_childminders_conf",
        ]
    ]
    la_data_nurse_childminder = (
        pd.merge(
            la_data,
            nurse_childminder_number_la,
            left_on="local_authority_code",
            right_on="LAD21CD",
            how="left",
        )
        .rename(columns={"RGN21CD": "region_code"})
        .drop(columns=["LAD21CD"])
    )

    logger.info("reorganise dataset to be region first...")
    cols_to_rearrange = la_data_nurse_childminder.columns.tolist()
    start_cols = ["local_authority", "local_authority_code", "region", "region_code"]
    cols_rearranged = start_cols + [
        col for col in cols_to_rearrange if col not in start_cols
    ]
    la_data_nurse_childminder = la_data_nurse_childminder[cols_rearranged]

    logger.info("save curated dataset to s3...")
    save_to_s3(
        BUCKET_NAME,
        la_data_nurse_childminder,
        "outputs/curated_data/curated_la_data.csv",
    )
    logger.info(
        "save cleaned regional- and LA- level 15-hour free childcare uptake data to s3..."
    )
    save_to_s3(
        BUCKET_NAME,
        b_eyp_percent_registered,
        "outputs/curated_data/curated_free_childcare_uptake_data.csv",
    )

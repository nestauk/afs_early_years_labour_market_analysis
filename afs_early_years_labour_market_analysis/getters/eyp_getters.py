"""
These getters relate to loading curated EYP data and other
    datasets that are used in the analysis.
"""
import pandas as pd
from afs_early_years_labour_market_analysis import BUCKET_NAME
from afs_early_years_labour_market_analysis.getters.data_getters import load_s3_data


def load_eyp_data() -> pd.DataFrame:
    """
    Loads curated EYP dataset that includings LA- and regional-
        level information on:
        - the number of early years providers;
        - number of children per age per local authority;
        - the regional number of nursery workers and childminders;
        - deprivation level and rural_urban information.

    Returns:
        pd.DataFrame: A pandas dataframe containing the curated EYP data
    """
    return load_s3_data(BUCKET_NAME, "outputs/curated_data/curated_la_data.csv")


def load_current_free_childcare_uptake_la() -> pd.DataFrame:
    """
    Loads cleaned free childcare uptake data at the LA- and region- level for different ages,
        different years and different types of childcare.

    Returns:
        pd.DataFrame: A pandas dataframe containing the free childcare uptake data at the regional and LA level
    """
    return load_s3_data(
        BUCKET_NAME, "outputs/curated_data/curated_free_childcare_uptake_data.csv"
    )


def load_current_free_childcare_uptake_national() -> pd.DataFrame:
    """
    Loads cleaned free childcare uptake data at the national level for different ages.

    Returns:
        pd.DataFrame: A pandas dataframe containing the free childcare uptake data at the national level
    """
    return load_s3_data(
        BUCKET_NAME,
        "inputs/ceyps/1c_early_years_provision_percentage_registered_national_2011_2022.csv",
    )


def load_free_childcare_by_provider_type_la() -> pd.DataFrame:
    """
    Loads registered children for 15- and 30-hour entitlement free childcare at the LA-level for different ages.

    Returns:
        pd.DataFrame: A pandas dataframe containing information on registered children for 15- and 30-hour entitlement free childcare at the LA-level for different ages.
    """
    return load_s3_data(
        BUCKET_NAME,
        "inputs/education_provision/1a_early_years_provision_children_registered_2018_2022.csv",
    )


# I know laura mentioned this as potentially helpful - not merging datasets with rows that are provider type specific
# not geography specific
def get_staff_turnover_rate_national() -> pd.DataFrame:
    """
    Loads national EYP staff turnover rate data based on different provider
        types.

    Returns:
        pd.DataFrame: A pandas dataframe containing the national EYP staff turnover rate data
    """
    return load_s3_data(BUCKET_NAME, "inputs/ceyps/sceyp22_staff_turnover_rev.csv")

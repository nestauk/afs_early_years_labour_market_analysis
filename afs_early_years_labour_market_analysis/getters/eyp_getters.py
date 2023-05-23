"""
These getters relate to loading curated EYP data
"""
import pandas as pd
from afs_early_years_labour_market_analysis import BUCKET_NAME
from afs_early_years_labour_market_analysis.getters.data_getters import load_s3_data


def load_eyp_data() -> pd.DataFrame:
    """Loads curated EYP dataset that includings LA- and regional-
        level information on:
        - the number of early years providers;
        - number of children per age per local authority;
        - the regional number of nursery workers and childminders;
        - deprivation level and rural_urban information.

    Returns:
        pd.DataFrame: A pandas dataframe containing the curated EYP data
    """
    return load_s3_data(BUCKET_NAME, "outputs/curated_data/curated_la_data.csv")

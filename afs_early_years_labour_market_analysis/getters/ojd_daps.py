"""
Getters for OJD DAPS data
"""
import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from typing import Mapping, Union, Dict, List
from afs_early_years_labour_market_analysis import BUCKET_NAME

def get_job_adverts() -> pd.DataFrame:
    """Returns dataframe of raw job adverts
    """
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/adverts_ojd_daps_extract.parquet",
        download_as="dataframe",
    )

def get_relevant_job_adverts() -> pd.DataFrame:
    """Returns dataframe of relevant job adverts
    """
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/relevant_job_adverts.parquet",
        download_as="dataframe",
    )
"""
Getters for OJD DAPS data
"""
import pandas as pd
from nesta_ds_utils.loading_saving.S3 import download_obj
from typing import Mapping, Union, Dict, List
from afs_early_years_labour_market_analysis import BUCKET_NAME


def get_job_adverts() -> pd.DataFrame:
    """Returns dataframe of raw job adverts"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/adverts_ojd_daps_extract.parquet",
        download_as="dataframe",
    )


def get_eyp_relevant_job_adverts() -> pd.DataFrame:
    """Returns dataframe of EYP job adverts"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/relevant_job_adverts_eyp.parquet",
        download_as="dataframe",
    )


def get_similar_job_adverts() -> pd.DataFrame:
    """Returns dataframe of similar job adverts to EYP jobs"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/relevant_job_adverts_sim_occs.parquet",
        download_as="dataframe",
    )


def get_salaries() -> pd.DataFrame:
    """Returns dataframe of salaries"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/salaries_ojd_daps_extract.parquet",
        download_as="dataframe",
    )


def get_locations() -> pd.DataFrame:
    """Returns dataframe of locations"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/locations_ojd_daps_extract.parquet",
        download_as="dataframe",
    )


def get_skills() -> pd.DataFrame:
    """Returns dataframe of skills"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/skills_ojd_daps_extract.parquet",
        download_as="dataframe",
    )


def get_eyp_relevant_enriched_job_adverts() -> pd.DataFrame:
    """Returns dataframe of relevant enriched job adverts for EYP job ads"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/enriched_relevant_job_adverts_eyp.parquet",
        download_as="dataframe",
    )


def get_similar_enriched_job_adverts() -> pd.DataFrame:
    """Returns dataframe of relevant enriched job adverts for similar job ads"""

    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/enriched_relevant_job_adverts_sim_occs.parquet",
        download_as="dataframe",
    )


def get_eyp_relevant_skills() -> pd.DataFrame:
    """Returns dataframe of relevant skills from EYP job ads"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/relevant_skills_eyp.parquet",
        download_as="dataframe",
    )


def get_similar_skills() -> pd.DataFrame:
    """Returns dataframe of relevant skills from similar job ads"""
    return download_obj(
        BUCKET_NAME,
        "inputs/ojd_daps_extract/relevant_skills_sim_occs.parquet",
        download_as="dataframe",
    )

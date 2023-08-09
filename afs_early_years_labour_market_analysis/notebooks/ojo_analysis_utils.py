"""
Functions and variables for use in the OJO analysis notebook.
"""
import re
import pandas as pd
from typing import Dict, Union
import os
from urllib.request import urlretrieve
from zipfile import ZipFile
import geopandas as gpd
import altair as alt
from typing import List, Dict
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity

from afs_early_years_labour_market_analysis import PROJECT_DIR

output_table_path = "outputs/ojo_analysis/report_tables/"

early_date_range = ("2021-07-01", "2022-07-01")
late_date_range = ("2022-07-01", "2023-07-01")

# for any qualification above 6, map to 6 as everything 6 and above is a degree
salary_mapper = {"inflation_adj_min_salary": "Min", "inflation_adj_max_salary": "Max"}

clean_salary_mapper = {
    "Median Minimum Annualised Salary (£)": "Min",
    "Median Maximum Annualised Salary (£)": "Max",
}
# Inflation rates for 2021, 2022 and 2023 (avg. monthly inflation rate from jan 2023 to march 2023)
# obtained from the OECD - https://data.oecd.org/price/inflation-cpi.htm (accessed 20/04/2021)
inflation_rate_dict = {"2020": 0.01, "2021": 0.025, "2022": 0.079, "2023": 0.0896667}

# Nuts file information
nuts_file = "NUTS_RG_20M_2021_4326_LEVL_3.geojson"
shapefile_path = "/afs_early_years_labour_market_analysis/notebooks/data/shapefiles/"
shape_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/download/ref-nuts-2021-20m.geojson.zip"

full_shapefile_path = str(PROJECT_DIR) + shapefile_path

map_color_range = ["darkred", "orange", "green"]


def is_england_geo(itl_3_code: str) -> bool:
    """Flag whether an itl 3 code is in England or not

    Args:
        itl_3_code (str): itl 3 code

    Returns:
        bool: True if in England, False otherwise
    """

    england_codes = "|".join(
        ["TLC", "TLD", "TLE", "TLF", "TLG", "TLH", "TLI", "TLJ", "TLK"]
    )
    england_codes_regex = f"^({england_codes})"

    if isinstance(itl_3_code, str) == False:
        return False

    matches = re.match(england_codes_regex, itl_3_code)
    if matches:
        return True
    else:
        return False


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


def get_nuts_geo_data(
    nuts_file: str = nuts_file,
    full_shapefile_path: str = full_shapefile_path,
    shape_url: str = shape_url,
) -> pd.DataFrame:
    """Get NUTS geo data for England

    Args:
        nuts_file (str, optional): Nuts file. Defaults to nuts_file.
        full_shapefile_path (str, optional): Full shape file path. Defaults to full_shapefile_path.
        shape_url (str, optional): Shape URL. Defaults to shape_url.

    Returns:
        pd.DataFrame: NUTS geo data for England
    """
    ##first get nuts shapefiles for the UK
    # get nutsfile
    if not os.path.exists(full_shapefile_path):
        os.makedirs(full_shapefile_path, exist_ok=True)

    zip_path, _ = urlretrieve(shape_url)
    with ZipFile(zip_path, "r") as zip_files:
        for zip_names in zip_files.namelist():
            if zip_names == nuts_file:
                zip_files.extract(zip_names, path=full_shapefile_path)
                nuts_geo = gpd.read_file(full_shapefile_path + nuts_file)
                nuts_geo = nuts_geo[nuts_geo["CNTR_CODE"] == "UK"].reset_index(
                    drop=True
                )

    # replace NUTS to ITL 3 code and only get England
    nuts_geo = (
        nuts_geo
        # convert to ITL 3 code
        .assign(id=lambda x: x["id"].str.replace("UK", "TL"))
        .assign(NUTS_ID=lambda x: x["NUTS_ID"].str.replace("UK", "TL"))
        # Make sure its only England
        .assign(is_england_geo=lambda x: x["NUTS_ID"].apply(is_england_geo))
        .query("is_england_geo == True")
    ).reset_index(drop=True)

    # Manually merge london nuts codes
    nuts_geo.loc[
        nuts_geo[nuts_geo["NUTS_ID"].str.startswith("TLI")].index, "NAME_LATN"
    ] = "London"
    nuts_geo.loc[
        nuts_geo[nuts_geo["NUTS_ID"].str.startswith("TLI")].index, "NUTS_NAME"
    ] = "London"
    nuts_geo.loc[
        nuts_geo[nuts_geo["NUTS_ID"].str.startswith("TLI")].index, "NUTS_ID"
    ] = "TLI"

    return nuts_geo


def generate_boxplot(
    box_plot_df: pd.DataFrame, facet_type: str, columns: int
) -> alt.Chart:
    """Generate boxplot of salary by different categorical variables

    Args:
        box_plot_df (pd.DataFrame): DataFrame to generate boxplot from
        facet_type (str): Type of facet to generate boxplot by
        columns (int): Number of columns to facet by
    """
    boxplot_graph = (
        alt.Chart(box_plot_df)
        .mark_boxplot(size=50, extent=0.5, outliers=False)
        .encode(
            alt.Y(
                "salary:Q", title="Annualised Salary (£)", scale=alt.Scale(zero=False)
            ),
            alt.X("salary_type:N", title="Salary Type"),
            alt.Color("salary_type:N", title="Salary Type"),
        )
        .facet(facet_type, columns=columns)
    )

    return boxplot_graph


def get_skill_similarity_scores(all_skills: pd.DataFrame) -> List[Dict[str, float]]:
    """Calculates the cosine similarity between the skill count
        vectors of an EYP and other sectors.

    Args:
        all_skills (pd.DataFrame): DataFrame of all skills

    Returns:
        List[Dict[str, float]]: List of dictionaries containing the sector and
            skill profile similarity score (cosine similarity)
    """
    # create count vectors of skills for each sector
    esco_id_2_id = all_skills.set_index("esco_id").esco_label.to_dict()
    sector_skill_dict = dict()
    for sector, skills in all_skills.groupby("sector"):
        skill_count = dict(Counter(skills.esco_id))
        for skill_id, indx in esco_id_2_id.items():
            if not skill_count.get(skill_id):
                skill_count[skill_id] = 0
        # sort the dictionary by skill id
        skill_count = dict(sorted(skill_count.items(), key=lambda item: item[0]))
        sector_skill_dict[sector] = skill_count

    # calculate cosine similarity between EYP and other sector skill count vectors
    eyp_count_vector = list(sector_skill_dict["Early Years Practitioner"].values())
    sector_sim = []
    for sector, skill_count in sector_skill_dict.items():
        if sector != "Early Years Practitioner":
            count_vector = list(skill_count.values())
            # calculate cosine similarity
            cosine_sim = cosine_similarity([eyp_count_vector], [count_vector])
            sector_sim.append(
                {"sector": sector, "skill_profile_similarity": cosine_sim}
            )

    return sector_sim

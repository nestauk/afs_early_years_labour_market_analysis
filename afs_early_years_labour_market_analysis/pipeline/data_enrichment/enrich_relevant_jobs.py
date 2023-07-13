"""
A flow to add relevant enrichment data to the relevant jobs datasets (both EYP and similar job adverts).

Adds:
- Salary
- Location
- Rural/urban classification
- Birth rate
- Skills (as a separate table)

For EYP jobs, we also add Qualification level

python afs_early_years_labour_market_analysis/pipeline/data_enrichment/enrich_relevant_jobs.py run
"""

from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import (
    get_eyp_relevant_job_adverts,
    get_similar_job_adverts,
    get_salaries,
    get_locations,
    get_skills,
)
from typing import Union
import spacy
from spacy.matcher import Matcher
import re
import pandas as pd

level_dict = {
    "qts": "6",
    "qtse": "6",
    "eyts": "6",
    "eyps": "6",
    "qualified teacher status": "6",
    "qualified teachers status": "6",
    "early years teacher status": "6",
    "early years professional status": "6",
    "ba": "6",
    "cert": "4",
    "pgce": "6",
    "degree": "6",
    "foundation": "5",
}

london_nuts_3 = [
    "UKI31",
    "UKI32",
    "UKI33",
    "UKI34",
    "UKI41",
    "UKI42",
    "UKI43",
    "UKI44",
    "UKI45",
    "UKI51",
    "UKI52",
    "UKI53",
    "UKI54",
    "UKI61",
    "UKI62",
    "UKI63",
    "UKI71",
    "UKI72",
    "UKI73",
    "UKI74",
    "UKI75",
]

patterns = [
    [{"LOWER": "level"}, {"IS_DIGIT": True}],
    [{"LOWER": "l"}, {"IS_DIGIT": True}],
    [{"LOWER": "levels"}, {"IS_DIGIT": True}],
    [{"LOWER": "levels"}, {"IS_DIGIT": True}, {"IS_PUNCT": True}, {"IS_DIGIT": True}],
    [{"LOWER": "level"}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "level"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "cache"}, {"IS_DIGIT": True}],
    [{"LOWER": "cache"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "nvq"}, {"IS_DIGIT": True}, {"POS": "CCONJ"}, {"IS_DIGIT": True}],
    [{"LOWER": "nvq"}, {"IS_DIGIT": True}],
    [{"LOWER": "ba"}, {"IS_SPACE": True}],
    [{"LOWER": "cert"}, {"IS_SPACE": True}],
    [{"LOWER": "degree"}],
    [{"LOWER": "qts"}],
    [{"LOWER": "qtse"}],
    [{"LOWER": "eyts"}],
    [{"LOWER": "eyps"}],
    [{"LOWER": "pgce"}],
    [{"LOWER": "foundation degree"}],
    [{"LOWER": "qualified teacher status"}],
    [{"LOWER": "qualified teachers status"}],
    [{"LOWER": "early years teacher status"}],
    [{"LOWER": "early years professional status"}],
]

nlp = spacy.load("en_core_web_sm")
matcher = Matcher(nlp.vocab)
matcher.add("qualification", patterns)


def get_qualification_level(job_description: str) -> Union[int, None]:
    """
    Function to extract qualification levels from a job description.

    Args:
        job_description (str): job description to extract qualification levels from.

    Returns:
        int: minimum qualification level mentioned in job description.
    """
    doc = nlp(job_description)
    matches = matcher(doc)

    qualification_level = []
    for match_id, start, end in matches:
        span = doc[start:end]  # The matched span
        span_text = span.text.lower()
        span_text_number = level_dict.get(span_text, span_text)
        # regex match numbers from span text
        numbers = re.findall(r"\d+", " ".join(span_text_number))
        qualification_level.extend(numbers)

    if qualification_level != []:
        return min([int(level) for level in qualification_level])
    else:
        return None


class EnrichRelevantJobs(FlowSpec):
    @step
    def start(self):
        """Start the flow."""
        self.next(self.get_data)

    @step
    def get_data(self):
        """Get relevant job adverts from OJO dataset."""
        # get relevant job adverts
        self.relevant_job_adverts_eyp = get_eyp_relevant_job_adverts()
        self.relevant_job_adverts_sim_occ = get_similar_job_adverts()
        # convert id to int
        self.relevant_job_adverts_eyp["id"] = self.relevant_job_adverts_eyp[
            "id"
        ].astype(int)
        self.relevant_job_adverts_sim_occ["id"] = self.relevant_job_adverts_sim_occ[
            "id"
        ].astype(int)
        # get enrichement data
        self.salaries = get_salaries()
        self.locations = get_locations()
        self.skills = get_skills()
        self.skills["id"] = self.skills["id"].astype(int)
        self.next(self.add_location_salaries)

    @step
    def add_location_salaries(self):
        """Add location and salary data to relevant job adverts."""
        self.eyp_enriched_relevant_job_adverts = (
            self.relevant_job_adverts_eyp.merge(
                self.salaries,
                on="id",
                how="left",
            )
            .merge(self.locations, on="id", how="left")
            .drop(columns=["job_location_raw_x"])
            .rename(columns={"job_location_raw_y": "job_location_raw"})
        )

        self.sim_enriched_relevant_job_adverts = (
            self.relevant_job_adverts_sim_occ.merge(
                self.salaries,
                on="id",
                how="left",
            )
            .merge(self.locations, on="id", how="left")
            .drop(columns=["job_location_raw_x"])
            .rename(columns={"job_location_raw_y": "job_location_raw"})
        )

        self.eyp_relevant_skills = self.relevant_job_adverts_eyp[["id"]].merge(
            self.skills, on="id", how="inner"
        )
        self.sim_relevant_skills = self.relevant_job_adverts_sim_occ[["id"]].merge(
            self.skills, on="id", how="inner"
        )
        self.next(self.add_qualification_level)

    @step
    def add_qualification_level(self):
        """Extract and add qualification level from job description for EYP-specific
        job adverts."""

        import afs_early_years_labour_market_analysis.utils.text_cleaning as tc

        ojd_jobs = pd.read_parquet(
            "s3://open-jobs-lake/latest_output_tables/descriptions.parquet"
        )
        print("Subsetting OJO job descriptions for EYP relevant job adverts...")

        eyp_job_ids = self.eyp_enriched_relevant_job_adverts.id.unique()

        eyp_jobs = (
            ojd_jobs.query("id in @eyp_job_ids")
            .assign(clean_description=lambda x: x.description.apply(tc.clean_text))
            .drop(columns=["description"])
        )
        print("Extracting qualification level from job descriptions...")
        eyp_clean_jobs_list = eyp_jobs.clean_description.unique().tolist()
        clean_job_qual_dict = {
            clean_job_desc: get_qualification_level(clean_job_desc)
            for clean_job_desc in eyp_clean_jobs_list[:10]
        }

        id2qual_dict = (
            eyp_jobs.assign(
                qualification_level=lambda x: x.clean_description.map(
                    clean_job_qual_dict
                )
            )
            .set_index("id")["qualification_level"]
            .T.to_dict()
        )
        self.eyp_enriched_relevant_job_adverts[
            "qualification_level"
        ] = self.eyp_enriched_relevant_job_adverts.id.map(id2qual_dict)

        self.next(self.add_location_metadata)

    @step
    def add_location_metadata(self):
        """Add additional location metadata to enriched datasets."""

        rural_urban_nuts = pd.read_csv(
            "s3://afs-early-years-labour-market-analysis/inputs/rural_urban_nuts.csv"
        )[["NUTS315CD", "RUC11CD", "RUC11", "Broad_RUC11"]].assign(
            itl_3_code=lambda x: x["NUTS315CD"].str.replace("UK", "TL")
        )

        # replace old nuts 3 codes for london to merged TL code
        itl_london_codes = rural_urban_nuts[
            rural_urban_nuts["NUTS315CD"].isin(london_nuts_3)
        ]["itl_3_code"].to_list()

        rural_urban_nuts["itl_3_code"] = rural_urban_nuts["itl_3_code"].replace(
            itl_london_codes, "TLI"
        )

        self.eyp_enriched_relevant_job_adverts_locmetadata = pd.merge(
            self.eyp_enriched_relevant_job_adverts,
            rural_urban_nuts,
            on="itl_3_code",
            how="left",
        ).drop(columns=["NUTS315CD"])
        self.sim_enriched_relevant_job_adverts_locmetadata = pd.merge(
            self.sim_enriched_relevant_job_adverts,
            rural_urban_nuts,
            on="itl_3_code",
            how="left",
        ).drop(columns=["NUTS315CD"])

        self.next(self.save_data)

    @step
    def save_data(self):
        # save to s3
        self.eyp_enriched_relevant_job_adverts_locmetadata.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/enriched_relevant_job_adverts_eyp.parquet",
            index=False,
        )
        self.eyp_relevant_skills.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_skills_eyp.parquet",
            index=False,
        )

        self.sim_enriched_relevant_job_adverts_locmetadata.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/enriched_relevant_job_adverts_sim_occs.parquet",
            index=False,
        )
        self.sim_relevant_skills.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_skills_sim_occs.parquet",
            index=False,
        )

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    EnrichRelevantJobs()

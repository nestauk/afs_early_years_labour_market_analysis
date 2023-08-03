"""
A flow to parse relevant Early Years jobs and similar jobs from the OJO dataset.

python afs_early_years_labour_market_analysis/pipeline/data_collection/refine_relevant_jobs.py run
"""
from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import get_job_adverts
from afs_early_years_labour_market_analysis.getters.data_getters import load_s3_data
from afs_early_years_labour_market_analysis import BUCKET_NAME
from afs_early_years_labour_market_analysis.utils.text_cleaning import clean_job_title

import re

# ------------------------------------------------ EYP JOB ADVERT QUERIES ---------------------------------------------------
eyp_occupation_titles = [
    "Nursery Nurse",
    "Nursery Practitioner",
    "Nursery Assistants",
    "Nursery Manager",
    "Early Practitioner",
    "Early Educator",
    "Nursery Teacher",
    "Early Teacher",
]

# Suggested job titles
eyp_job_titles = [
    "early years teacher",
    "early years practitioners",
    "early years educator",
    "early years early career teacher (ect)",
    "early years assessor",
    "early years assistant",
    "nursery practitioner",
    "nursery nurse",
    "nursery manager",
    "nursery assistant",
    "preschool assistant",
    "preschool manager",
    "nursery preschool assistant",
    "nursery senior room leader",
    "baby room teacher",
    "level 3 childcare practitioner",
    "early years apprentice",
    "early years deputy manager",
    "nursery school classroom teacher",
    "nursery officer",
    "early years and family practitioner",
    "deputy nursery manager",
    "room leader",
    "qualified practitioner",
    "early years teaching assistant",
]

# ------------------------------------------------ SIMILAR JOB ADVERT QUERIES ---------------------------------------------------

job_titles_to_match_on = [
    "teaching assistant",
    "sen teaching assistant",
    "sen teacher assistant" "primary school teacher",
    "primary teacher",
    "special needs teacher",
    "sen teacher",
    "secondary school teacher",
    "secondary teacher",
    "waiter",
    "waitress",
    "retail assistant",
    "store assistant",
    "shop assistant",
    "supply teacher",
]

job_title_group_mapper = {
    "teaching assistant": "Teaching Assistant",
    "sen teaching assistant": "Teaching Assistant",
    "sen teacher assistant": "Teaching Assistant",
    "primary school teacher": "Primary School Teacher",
    "primary teacher": "Primary School Teacher",
    "special needs teacher": "Special Needs Teacher",
    "sen teacher": "Special Needs Teacher",
    "secondary school teacher": "Secondary School Teacher",
    "secondary teacher": "Secondary School Teacher",
    "waiter": "Waiter",
    "waitress": "Waiter",
    "retail assistant": "Retail Assistant",
    "store assistant": "Retail Assistant",
    "shop assistant": "Retail Assistant",
    "supply teacher": "Supply Teacher",
}

# manually removed headteacher, deputy head or assistant headteacher from occupation titles related to teaching
relevant_occupations = [
    "Teacher Assistant",
    "Primary Teacher",
    "Design Teacher",
    "Graduates Teacher",
    "Art Teacher",
    "Supply Teachers",
    "Spanish Teacher",
    "Education Teacher",
    "Teacher School",
    "Secondary Teacher",
    "Stage Teacher",
    "Teacher Permanent",
    "Early Teacher",
    "Special Teacher",
    "English Teacher",
    "Business Teacher",
    "Maths Teacher",
    "Teacher Level",
    "Apprentice Teacher",
    "History Teacher",
    "Technology Teacher",
    "Mathematics Teacher",
    "Science Teacher",
    "Teacher Cover",
    "Nursery Teacher",
    "Lecturer Teacher",
    "Humanities Teacher",
    "Psychology Teacher",
    "French Teacher",
    "Music Teacher",
    "Reception Teacher",
    "Qualified Teachers",
    "Economics Teacher",
    "Teacher Students",
    "Care Teacher",
    "Pmld Teacher",
    "Geography Teacher",
    "Drama Teacher",
    "Teacher Tutor",
    "Intervention Teacher",
    "Teaching Assistant",
    "Mfl Teacher",
    "Studies Teacher",
    "Preparation Teacher",
    "Specialist Teacher",
    "Languages Teacher",
    "Lower Teacher",
    "Teacher Programme",
    "Pupil Teacher",
    "Teacher Mentor",
    "Disorders Teacher",
    "Physics Teacher",
    "Recruiting Teacher",
    "Teacher Training",
    "Materials Teacher",
    "Teacher Tlr",
    "P.E Teacher",
    "Teacher Easter",
    "Media Teacher",
    "Worker Teacher",
    "Computing Teacher",
    "Sociology Teacher",
    "Biology Teacher",
    "Engineering Teacher",
    "Teacher Coordinator",
    "Food Teacher",
    "Booster Teacher",
    "Teacher Support",
    "Teach English",
    "German Teacher",
    "Teacher Long",
    "Teach Support",
    "Teacher Day",
    "Teacher Performing",
    "Teacher Open",
    "Teacher Starting",
    "Nurse Teacher",
    "Foundation Teacher",
    "Teach Science",
    "Textiles Teacher",
    "Teacher Outstanding",
    "Teach Maths",
]

relevant_knowledge_domains = ["Hospitality And Catering", "Education"]

relevant_parent_sectors = ["Retail", "Education", "Hospitality &amp; Catering"]

relevant_sectors = [
    "Other Education",
    "Supply Teacher",
    "Teaching Assistant",
    "Other Retail",
    "Sales Assistant",
    "Waiting &amp; Bar Staff",
    "Supply Teacher",
    "Primary School",
    "Secondary School",
    "Special Needs",
]


class RefineRelevantJobs(FlowSpec):
    @step
    def start(self):
        """Start the flow."""
        self.next(self.get_job_adverts)

    @step
    def get_job_adverts(self):
        """Get job adverts from OJO dataset."""
        self.job_adverts = get_job_adverts()
        self.next(self.refine_relevant_jobs)

    @step
    def refine_relevant_jobs(self):
        """Refine relevant jobs from OJO dataset."""
        self.relevant_job_adverts_eyp = self.job_adverts[
            (self.job_adverts["job_title_raw"].str.lower().isin(eyp_job_titles))
            | (self.job_adverts["occupation"].isin(eyp_occupation_titles))
            | (self.job_adverts["sector"].str.lower().str.contains("nursery"))
            | (
                self.job_adverts["job_title_raw"]
                .str.lower()
                .str.contains("early years")
            )
        ]
        self.relevant_job_adverts_eyp["sector"] = "Early Years Practitioner"
        print(f"the shape of the EYP data is: {self.relevant_job_adverts_eyp.shape}")

        # 1 -- query job adverts to make sure they are in relevant domains and sectors
        sim_job_adverts = self.job_adverts[
            (self.job_adverts["occupation"].isin(relevant_occupations))
            | (self.job_adverts["knowledge_domain"].isin(relevant_knowledge_domains))
            | (self.job_adverts["sector"].isin(relevant_sectors))
            | (self.job_adverts["parent_sector"].isin(relevant_parent_sectors))
        ]
        sim_job_adverts["clean_job_title"] = sim_job_adverts.job_title_raw.apply(
            clean_job_title
        )

        # 2 -- query job adverts to make sure they are in relevant job titles
        for matched_job_title in job_titles_to_match_on:
            sim_job_adverts.loc[
                sim_job_adverts.clean_job_title.str.contains(matched_job_title),
                "matched_job_title",
            ] = matched_job_title

        # 3 -- tidy up relevant job adverts
        sim_job_adverts = (
            sim_job_adverts.query("matched_job_title.notnull()")
            # clean up sector names with the job title group mapper - we're not really
            # using sectors, we're just using job titles to compare EYP with.
            .assign(sector=lambda x: x.matched_job_title.map(job_title_group_mapper))
            # drop any job titles that have the word 'trainee' or 'aspiring' in
            .query('clean_job_title.str.contains("trainee") == False').query(
                'clean_job_title.str.contains("aspiring") == False'
            )
        ).reset_index(drop=True)

        # 4 -- make sure eyp job ads are not in sim occ jobs
        eyp_job_ids = self.relevant_job_adverts_eyp.id.astype(str).to_list()
        self.relevant_job_adverts_sim_occs_no_eyp = sim_job_adverts[
            ~sim_job_adverts.id.astype(str).isin(eyp_job_ids)
        ]

        print(
            f"the shape of similar jobs data is: {self.relevant_job_adverts_sim_occs_no_eyp.shape}"
        )

        self.relevant_job_adverts_eyp.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_job_adverts_eyp.parquet",
            index=False,
        )
        self.relevant_job_adverts_sim_occs_no_eyp.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_job_adverts_sim_occs.parquet",
            index=False,
        )
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    RefineRelevantJobs()

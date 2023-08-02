"""
A flow to parse relevant Early Years jobs and similar jobs from the OJO dataset.

python afs_early_years_labour_market_analysis/pipeline/data_collection/refine_relevant_jobs.py run
"""
from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import get_job_adverts
from afs_early_years_labour_market_analysis.getters.data_getters import load_s3_data
from afs_early_years_labour_market_analysis import BUCKET_NAME

from itertools import chain

# load most similar occupation names based on Karlis's transitions algorithm

sim_job_titles = list(
    chain(*load_s3_data(BUCKET_NAME, "inputs/similar_occupations.txt"))
) + [
    "primary teacher",
    "secondary school teacher",
    "teaching assistant",
    "special educational needs teaching assistant",
    "special education teacher",
    "special education needs teaching asistant",
    "montessori teacher",
]

##add sales assistant + hospitality job titles to list
other_sim_titles = [
    "retail assistant",
    "shop assistant",
    "sale assistant",
    "sales assistant",
    "store assistant",
    "retail store assistant",
    "shop floor assistant",
    "volunteer shop assistant",
    "volunteer shop floor assistant",
    "waiter",
    "waitress",
    "team member - restaurant",
    "team member - night inspection",
    "waiting staff",
]

all_sim_job_titles = [
    job_title.lower() for job_title in sim_job_titles
] + other_sim_titles

# ditto for occupations
occupation_titles = [
    "Retail Assistant",
    "Retail Sales",
    "Store Assistant",
    "Sales Assistant",
    "Supervisor Store",
    "Supervisor Store",
    "other hospitality &amp; catering",
    "key stage 3 &amp; 4",
    "key stage 2",
    "key stage 5",
    "key stage 1",
    "teaching assistant",
    "secondary school",
    "primary school",
    "special needs",
]

# Most common job titles in the "Nursery" sector
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
        print(f"the shape of the EYP data is: {self.relevant_job_adverts_eyp.shape}")

        relevant_job_adverts_sim_occs = self.job_adverts[
            (self.job_adverts["job_title_raw"].str.lower().isin(all_sim_job_titles))
            | (self.job_adverts["occupation"].str.lower().isin(occupation_titles))
            | (
                self.job_adverts["sector"]
                .str.lower()
                .str.contains("teaching assistant")
            )
        ]

        # make sure eyp job ads are not in sim occ jobs
        eyp_job_ids = self.relevant_job_adverts_eyp.id.astype(str).to_list()
        self.relevant_job_adverts_sim_occs_no_eyp = relevant_job_adverts_sim_occs[
            ~relevant_job_adverts_sim_occs.id.astype(str).isin(eyp_job_ids)
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

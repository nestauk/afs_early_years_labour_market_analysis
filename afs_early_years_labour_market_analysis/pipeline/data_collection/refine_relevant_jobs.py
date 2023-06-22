import sys

sys.path.append(
    "/Users/india.kerlenesta/Projects/afs_early_years_labour_market_analysis"
)
"""
A flow to parse relevant Early Years jobs and shop assistant jobs from the OJO dataset.

python afs_early_years_labour_market_analysis/pipeline/data_collection/refine_relevant_jobs.py run
"""
from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import get_job_adverts

shop_job_titles = [
    "retail assistant",
    "shop assistant",
    "sale assistant",
    "sales assistant",
    "store assistant",
    "retail store assistant",
    "shop floor assistant",
    "volunteer shop assistant",
    "volunteer shop floor assistant",
]

# most common occs in the "Store Staff" sector
shop_occupation_titles = [
    "Retail Assistant",
    "Retail Sales",
    "Store Assistant",
    "Sales Assistant",
    "Supervisor Store",
    "Supervisor Store",
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

        self.relevant_job_adverts_shops = self.job_adverts[
            (self.job_adverts["job_title_raw"].str.lower().isin(shop_job_titles))
            | (self.job_adverts["occupation"].isin(shop_occupation_titles))
            | (self.job_adverts["sector"].str.lower().str.contains("store staff"))
        ]

        print(
            f"the shape of the shop assistant data is: {self.relevant_job_adverts_shops.shape}"
        )

        self.relevant_job_adverts_eyp.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_job_adverts_eyp.parquet",
            index=False,
        )
        self.relevant_job_adverts_shops.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_job_adverts_shop.parquet",
            index=False,
        )
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    RefineRelevantJobs()

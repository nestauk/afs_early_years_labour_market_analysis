"""
A flow to add relevant enrichment data to the relevant jobs dataset.

Adds:
- Salary
- Location
- Skills (as a separate table)
"""

from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import (
    get_relevant_job_adverts,
    get_salaries,
    get_locations,
    get_skills
)

class EnrichRelevantJobs(FlowSpec):
    @step
    def start(self):
        """Start the flow."""
        self.next(self.get_data)

    @step
    def get_data(self):
        """Get relevant job adverts from OJO dataset."""
        self.relevant_job_adverts = get_relevant_job_adverts()
        self.relevant_job_adverts["id"] = self.relevant_job_adverts["id"].astype(int)
        self.salaries = get_salaries()
        self.locations = get_locations()
        self.skills = get_skills()
        self.skills["id"] = self.skills["id"].astype(int)
        self.next(self.enrich_relevant_jobs)

    @step
    def enrich_relevant_jobs(self):
        self.enriched_relevant_job_adverts = self.relevant_job_adverts.merge(
            self.salaries, on="id", how="left",
        ).merge(
            self.locations, on="id", how="left")
        self.relevant_skills =self.relevant_job_adverts[["id"]].merge(self.skills, on="id", how="inner")
        print(self.enriched_relevant_job_adverts.shape)
        print(self.relevant_skills.shape)
        # save to s3
        self.enriched_relevant_job_adverts.to_parquet("s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/enriched_relevant_job_adverts.parquet", index=False)
        self.relevant_skills.to_parquet("s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_skills.parquet", index=False)
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass

if __name__ == "__main__":
    EnrichRelevantJobs()

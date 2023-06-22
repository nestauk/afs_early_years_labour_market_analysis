import sys

sys.path.append(
    "/Users/india.kerlenesta/Projects/afs_early_years_labour_market_analysis"
)
"""
A flow to add relevant enrichment data to the relevant jobs dataset.

Adds:
- Salary
- Location
- Skills (as a separate table)

python afs_early_years_labour_market_analysis/pipeline/data_collection/enrich_relevant_jobs.py run
"""

from metaflow import FlowSpec, step, Parameter

from afs_early_years_labour_market_analysis.getters.ojd_daps import (
    get_eyp_relevant_job_adverts,
    get_shop_relevant_job_adverts,
    get_salaries,
    get_locations,
    get_skills,
)


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
        self.relevant_job_adverts_shop = get_shop_relevant_job_adverts()
        # convert id to int
        self.relevant_job_adverts_eyp["id"] = self.relevant_job_adverts_eyp[
            "id"
        ].astype(int)
        self.relevant_job_adverts_shop["id"] = self.relevant_job_adverts_shop[
            "id"
        ].astype(int)
        # get enrichement data
        self.salaries = get_salaries()
        self.locations = get_locations()
        self.skills = get_skills()
        self.skills["id"] = self.skills["id"].astype(int)
        self.next(self.enrich_relevant_jobs)

    @step
    def enrich_relevant_jobs(self):
        self.eyp_enriched_relevant_job_adverts = self.relevant_job_adverts_eyp.merge(
            self.salaries,
            on="id",
            how="left",
        ).merge(self.locations, on="id", how="left")
        self.shop_enriched_relevant_job_adverts = self.relevant_job_adverts_shop.merge(
            self.salaries,
            on="id",
            how="left",
        ).merge(self.locations, on="id", how="left")
        self.eyp_relevant_skills = self.relevant_job_adverts_eyp[["id"]].merge(
            self.skills, on="id", how="inner"
        )
        self.shop_relevant_skills = self.relevant_job_adverts_shop[["id"]].merge(
            self.skills, on="id", how="inner"
        )

        # save to s3
        self.eyp_enriched_relevant_job_adverts.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/enriched_relevant_job_adverts_eyp.parquet",
            index=False,
        )
        self.eyp_relevant_skills.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_skills_eyp.parquet",
            index=False,
        )

        self.shop_enriched_relevant_job_adverts.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/enriched_relevant_job_adverts_shop.parquet",
            index=False,
        )
        self.shop_relevant_skills.to_parquet(
            "s3://afs-early-years-labour-market-analysis/inputs/ojd_daps_extract/relevant_skills_shop.parquet",
            index=False,
        )

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    EnrichRelevantJobs()

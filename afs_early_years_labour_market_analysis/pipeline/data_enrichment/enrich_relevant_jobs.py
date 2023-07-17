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
import pandas as pd


class EnrichRelevantJobs(FlowSpec):
    # add a parameter to specify chunk size
    chunk_size = Parameter("chunk_size", help="Chunk size for processing", default=1000)
    # add a boolean parameter to define if flow should be written in production or not
    production = Parameter(
        "production", help="Run flow in production mode", default=True
    )

    @step
    def start(self):
        """Start the flow."""
        self.next(self.get_data)

    @step
    def get_data(self):
        """Get and prepare relevant job adverts from OJO dataset."""
        from afs_early_years_labour_market_analysis.getters.ojd_daps import (
            get_eyp_relevant_job_adverts,
            get_similar_job_adverts,
            get_salaries,
            get_locations,
            get_skills,
        )
        import afs_early_years_labour_market_analysis.utils.text_cleaning as tc

        # get relevant job adverts
        print("Loading relevant job adverts...")
        self.relevant_job_adverts_eyp = get_eyp_relevant_job_adverts()
        self.relevant_job_adverts_sim_occ = get_similar_job_adverts()

        # get enrichement data
        print("Loading enrichment data...")
        self.salaries = get_salaries()
        self.locations = get_locations()
        self.skills = get_skills()

        # convert id to int
        for df in [
            self.relevant_job_adverts_eyp,
            self.relevant_job_adverts_sim_occ,
            self.skills,
        ]:
            df["id"] = df["id"].astype(int)

        print("Loading job description data...")
        ojd_jobs = pd.read_parquet(
            "s3://open-jobs-lake/latest_output_tables/descriptions.parquet"
        )

        print("Subsetting OJO job descriptions for EYP relevant job adverts...")

        eyp_job_ids = self.relevant_job_adverts_eyp.id.unique()

        self.eyp_jobs = (
            ojd_jobs.query("id in @eyp_job_ids")
            .assign(clean_description=lambda x: x.description.apply(tc.clean_text))
            .drop(columns=["description"])
        )

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

        self.next(self.generate_job_description_list)

    @step
    def generate_job_description_list(self):
        """Generate job description chunks to extract
        qualification level for EYP-specific job adverts."""
        import toolz

        self.eyp_clean_jobs_list = self.eyp_jobs.clean_description.unique().tolist()

        self.eyp_clean_jobs_list if self.production else self.eyp_clean_jobs_list[
            : self.chunk_size
        ]

        self.eyp_chunks = list(
            toolz.partition_all(self.chunk_size, self.eyp_clean_jobs_list)
        )

        self.next(self.extract_qualification_level, foreach="eyp_chunks")

    @step
    def extract_qualification_level(self):
        """Extract qualification levels per job description chunk"""
        import afs_early_years_labour_market_analysis.utils.data_enrichment as de

        self.clean_job_quals = [
            de.get_qualification_level(clean_job_desc) for clean_job_desc in self.input
        ]

        self.next(self.join)

    @step
    def join(self, inputs):
        """Join qualification level data to EYP-specific job adverts."""
        import itertools

        qual_list = list(itertools.chain(*[i.clean_job_quals for i in inputs]))

        self.merge_artifacts(inputs, exclude=["clean_job_quals"])

        desc2qual_dict = dict(zip(self.eyp_clean_jobs_list, qual_list))

        id2qual_dict = (
            self.eyp_jobs.assign(
                qualification_level=lambda x: x.clean_description.map(desc2qual_dict)
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
        import afs_early_years_labour_market_analysis.utils.data_enrichment as de

        rural_urban_nuts = (
            pd.read_csv(
                "s3://afs-early-years-labour-market-analysis/inputs/rural_urban_nuts.csv"
            )[["NUTS315CD", "RUC11CD", "RUC11", "Broad_RUC11"]]
            .assign(itl_3_code=lambda x: x["NUTS315CD"].str.replace("UK", "TL"))
            .rename(
                columns={
                    "RUC11CD": "ruc11_code",
                    "RUC11": "ruc11",
                    "Broad_RUC11": "broad_ruc11",
                }
            )
        )

        # replace old nuts 3 codes for london to merged TL code
        itl_london_codes = rural_urban_nuts[
            rural_urban_nuts["NUTS315CD"].isin(de.london_nuts_3)
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
        if self.production:
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
        else:
            pass

        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        pass


if __name__ == "__main__":
    EnrichRelevantJobs()

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
    @step
    def start(self):
        """Start the flow."""
        self.next(self.get_data)

    @step
    def get_data(self):
        """Get and prepare relevant datasets."""
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

        self.rural_urban_nuts = (
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

        self.next(self.enrich_data)

    @step
    def enrich_data(self):
        """Add location, salary and qualification levels to relevant job adverts."""
        import afs_early_years_labour_market_analysis.utils.data_enrichment as de

        print("Adding location and salaries information...")
        print("adding itl code and salary information for EYP jobs...")
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
        # add clean descriptions here from eyp_jobs by merging the two dataframes on id
        print("adding clean descriptions for EYP jobs...")
        self.eyp_enriched_relevant_job_adverts = (
            self.eyp_enriched_relevant_job_adverts.merge(
                self.eyp_jobs, on="id", how="left"
            )
        )
        print("adding itl code and salary information for similar jobs...")
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

        # replace old nuts 3 codes for london to merged TL code
        print(
            "adding rural/urban classification information for EYP and similar jobs..."
        )
        itl_london_codes = self.rural_urban_nuts[
            self.rural_urban_nuts["NUTS315CD"].isin(de.london_nuts_3)
        ]["itl_3_code"].to_list()

        self.rural_urban_nuts["itl_3_code"] = self.rural_urban_nuts[
            "itl_3_code"
        ].replace(itl_london_codes, "TLI")
        # this is where the bug was - drop duplicates on itl_3_code
        self.rural_urban_nuts.drop_duplicates(subset=["itl_3_code"], inplace=True)

        self.eyp_enriched_relevant_job_adverts_locmetadata = pd.merge(
            self.eyp_enriched_relevant_job_adverts,
            self.rural_urban_nuts,
            on="itl_3_code",
            how="left",
        ).drop(columns=["NUTS315CD"])

        print("extracting qualification level for EYP jobs...")
        self.eyp_enriched_relevant_job_adverts_locmetadata[
            "qualification_level"
        ] = self.eyp_enriched_relevant_job_adverts_locmetadata.clean_description.apply(
            de.get_qualification_level
        )

        self.sim_enriched_relevant_job_adverts_locmetadata = pd.merge(
            self.sim_enriched_relevant_job_adverts,
            self.rural_urban_nuts,
            on="itl_3_code",
            how="left",
        ).drop(columns=["NUTS315CD"])

        print("getting skills for EYP and similar jobs...")

        self.eyp_relevant_skills = self.relevant_job_adverts_eyp[["id"]].merge(
            self.skills, on="id", how="inner"
        )
        self.sim_relevant_skills = self.relevant_job_adverts_sim_occ[["id"]].merge(
            self.skills, on="id", how="inner"
        )

        print("enrichment complete!")

        self.next(self.save_data)

    @step
    def save_data(self):
        """Save enriched datasets to s3."""
        # save to s3
        self.eyp_enriched_relevant_job_adverts_locmetadata.drop(
            columns=["clean_description"], inplace=True
        )

        if self.production:
            print("saving data...")
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

"""
Script to filter enriched datasets to use for OJO analysis and streamlit app

python -m afs_early_years_labour_market_analysis.pipeline.data_enrichment.filter_relevant_jobs
"""
from afs_early_years_labour_market_analysis import PROJECT_DIR
import afs_early_years_labour_market_analysis.analysis.analysis_utils as au

import os
import pandas as pd

from typing import List, Dict
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity

analysis_data_path = (
    PROJECT_DIR / "afs_early_years_labour_market_analysis/analysis/data"
)

if not os.path.exists(analysis_data_path):
    os.makedirs(analysis_data_path)


def get_skill_similarity_scores(all_skills: pd.DataFrame) -> List[Dict[str, float]]:
    """Calculates the cosine similarity between the skill count
        vectors of an EYP and other professions.

    Args:
        all_skills (pd.DataFrame): DataFrame of all skills

    Returns:
        List[Dict[str, float]]: List of dictionaries containing the profession and
            skill profile similarity score (cosine similarity)
    """
    # create count vectors of skills for each profession
    esco_id_2_id = all_skills.set_index("esco_id").esco_label.to_dict()
    profession_skill_dict = dict()
    for profession, skills in all_skills.groupby("profession"):
        skill_count = dict(Counter(skills.esco_id))
        for skill_id, indx in esco_id_2_id.items():
            if not skill_count.get(skill_id):
                skill_count[skill_id] = 0
        # sort the dictionary by skill id
        skill_count = dict(sorted(skill_count.items(), key=lambda item: item[0]))
        profession_skill_dict[profession] = skill_count

    # calculate cosine similarity between EYP and other profession skill count vectors
    eyp_count_vector = list(profession_skill_dict["Early Years Practitioner"].values())
    profession_sim = []
    for profession, skill_count in profession_skill_dict.items():
        if profession != "Early Years Practitioner":
            count_vector = list(skill_count.values())
            # calculate cosine similarity
            cosine_sim = cosine_similarity([eyp_count_vector], [count_vector])
            profession_sim.append(
                {"profession": profession, "skill_profile_similarity": cosine_sim}
            )

    return profession_sim


if __name__ == "__main__":
    print("Loading relevant datasets...")
    eyp_jobs_clean = pd.read_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/eyp_jobs_clean.csv"
    )
    sim_jobs_clean = pd.read_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/sim_jobs_clean.csv"
    )
    all_skills = pd.read_csv(
        "s3://afs-early-years-labour-market-analysis/outputs/curated_data/all_skills_clean.csv"
    )
    all_jobs_clean = pd.concat([eyp_jobs_clean, sim_jobs_clean])

    print("creating facts and figures df...")
    # Median annualised salary and advertisement count per profession table
    median_salary_profession = (
        all_jobs_clean.groupby(["profession", "nation"])
        .agg(
            {
                "id": "count",
                "inflation_adj_min_salary": "median",
                "inflation_adj_max_salary": "median",
            }
        )
        .reset_index()
    )
    # save table to s3
    median_salary_profession = median_salary_profession.astype(
        {
            "inflation_adj_min_salary": "int",
            "inflation_adj_max_salary": "int",
        }
    )

    median_salary_profession.to_csv(
        os.path.join(
            analysis_data_path, "median_salary_count_per_profession_table.csv"
        ),
        index=False,
    )

    # Median annualised salary per qualification level table

    median_salary_qualification = (
        eyp_jobs_clean.groupby(["qualification_level", "nation"])
        .agg(
            {
                "id": "count",
                "inflation_adj_min_salary": "median",
                "inflation_adj_max_salary": "median",
            }
        )
        .reset_index()
    )

    median_salary_qualification.to_csv(
        os.path.join(analysis_data_path, "median_salary_count_per_qual_table.csv"),
        index=False,
    )

    # Create qualification level salary df
    print("creating qualification level salary boxplot df...")
    box_plot_df = eyp_jobs_clean.query('itl_3_name != "London"').melt(
        id_vars=["profession", "qualification_level", "nation"],
        value_vars=["inflation_adj_min_salary", "inflation_adj_max_salary"],
        var_name="salary_type",
        value_name="salary",
    )
    box_plot_df = box_plot_df[box_plot_df["qualification_level"].isin([2, 3, 6])]
    box_plot_df = (
        box_plot_df.dropna(subset=["qualification_level"])
        .assign(salary_type=lambda x: x.salary_type.replace(au.salary_mapper))
        .rename(columns={"qualification_level": "Qualification Level"})
    )
    box_plot_df.to_csv(
        analysis_data_path / "qualification_level_salary_boxplot.csv", index=False
    )

    # Create wage ratio df
    print("Creating wage ratio df...")
    # Create qualification level salary df
    eyp_jobs_clean_no_london = eyp_jobs_clean.query('itl_3_name != "London"')
    eyp_jobs_clean["min_wage_ratio"] = (
        eyp_jobs_clean["inflation_adj_min_salary"]
        / eyp_jobs_clean.inflation_adj_min_salary.median()
    )
    eyp_jobs_clean["max_wage_ratio"] = (
        eyp_jobs_clean["inflation_adj_max_salary"]
        / eyp_jobs_clean.inflation_adj_max_salary.median()
    )

    wage_ratio_df = (
        eyp_jobs_clean.groupby(["qualification_level", "nation"])
        .agg({"id": "count", "min_wage_ratio": "median", "max_wage_ratio": "median"})
        .reset_index()
        .rename(columns={"id": "count"})
    )
    wage_ratio_df = wage_ratio_df[wage_ratio_df.qualification_level.isin([2, 3, 6])]
    wage_ratio_df["requires_degree"] = False
    wage_ratio_df.loc[
        wage_ratio_df.query("qualification_level == 6").index, "requires_degree"
    ] = True

    wage_ratio_df.to_csv(analysis_data_path / "wage_ratio_df.csv", index=False)

    print("Creating profession salary boxplot df...")
    box_plot_df = all_jobs_clean.query('itl_3_name != "London"').melt(
        id_vars=["profession", "nation"],
        value_vars=["inflation_adj_min_salary", "inflation_adj_max_salary"],
        var_name="salary_type",
        value_name="salary",
    )
    box_plot_df = box_plot_df.assign(
        salary_type=lambda x: x.salary_type.replace(au.salary_mapper)
    ).rename(columns={"profession": "profession"})
    box_plot_df.to_csv(
        analysis_data_path / "salary_by_profession_boxplot.csv", index=False
    )

    print("Creating rolling monlthy salary by profession df...")
    # Generate Median Salary by profession Over Time Graph
    monthly_profession_sal_count = (
        all_jobs_clean.groupby(["profession", "month_year", "nation"])
        .agg(
            {"inflation_adj_min_salary": "median", "inflation_adj_max_salary": "median"}
        )
        .reset_index()
        .rename(
            columns={
                "inflation_adj_min_salary": "Median Minimum Annualised Salary (£)",
                "inflation_adj_max_salary": "Median Maximum Annualised Salary (£)",
            }
        )
    )

    monthly_profession_sal_count_melt = monthly_profession_sal_count.melt(
        id_vars=["profession", "month_year", "nation"],
        value_vars=[
            "Median Minimum Annualised Salary (£)",
            "Median Maximum Annualised Salary (£)",
        ],
        var_name="salary_type",
        value_name="salary",
    )
    monthly_profession_sal_count_melt.salary_type = (
        monthly_profession_sal_count_melt.salary_type.map(au.clean_salary_mapper)
    )
    # Current 2023 median salary for England
    monthly_profession_sal_count_melt["median_salary"] = 29588
    monthly_profession_sal_count_melt.to_csv(
        analysis_data_path / "monthly_profession_sal_count.csv", index=False
    )

    print("Creating rolling monthly job count by profession df...")
    profession_count_created = (
        all_jobs_clean.groupby(["profession", "created", "nation"])
        .size()
        .reset_index(name="count")
    )
    profession_count_created["created"] = pd.to_datetime(
        profession_count_created["created"]
    )
    # Set 'created' as the index to use resample
    profession_count_created.set_index("created", inplace=True)

    # # Calculate rolling mean per profession
    monthly_mean = (
        profession_count_created.groupby(["profession", "nation"])["count"]
        .rolling("30D")
        .mean()
        .reset_index()
    )
    monthly_mean.to_csv(analysis_data_path / "monthly_profession_mean.csv", index=False)

    print("Creating skill sims df...")
    nations = list(all_skills.query("~nation.isna()").nation.unique())
    profession_sim_dfs = []
    for nation in nations:
        sims = get_skill_similarity_scores(all_skills.query(f'nation == "{nation}"'))
        nation_count = all_skills.query(f'nation == "{nation}"').id.nunique()
        sims_clean = [
            {
                "nation": nation,
                "profession": x["profession"],
                "skill_profile_similarity": x["skill_profile_similarity"][0][0],
            }
            for x in sims
        ]
        profession_sim_dfs.append(sims_clean)
    profession_sim_df = pd.DataFrame(
        [item for sublist in profession_sim_dfs for item in sublist]
    )
    # round the values to 2 decimal places
    profession_sim_df["skill_profile_similarity"] = profession_sim_df[
        "skill_profile_similarity"
    ].round(2)
    profession_sim_df.to_csv(
        analysis_data_path / "skill_profile_similarity.csv", index=False
    )

    print("Creating top skills per profession df...")
    skill2name_mapper = all_skills.set_index("esco_id").esco_label.to_dict()
    all_skills_count = (
        all_skills.query("esco_id.str.len() > 10")
        .groupby(["profession", "esco_id", "nation"])
        .size()
        .reset_index()
        .rename(columns={0: "count"})
    )

    top_skills_per_profession_and_nation = []

    # First, group by both profession and nation
    grouped_data = all_skills_count.groupby(["profession", "nation"])

    for (profession, nation), group in grouped_data:
        # Calculate the total number of job ads for the current profession and nation combination
        total_job_ads = all_skills[
            (all_skills["profession"] == profession) & (all_skills["nation"] == nation)
        ]["id"].nunique()

        # Calculate the job ad percentage for each skill in the current group
        group["job_ad_percent"] = (group["count"] / total_job_ads) * 100

        # Get the top 10 skills by job ad percentage
        top_skills_count = group.sort_values(by="job_ad_percent", ascending=False).head(
            10
        )

        # Append the result to the list
        top_skills_per_profession_and_nation.append(top_skills_count)

    top_skills_per_profession_and_nation_df = pd.concat(
        top_skills_per_profession_and_nation
    ).reset_index(drop=True)
    top_skills_per_profession_and_nation_df[
        "esco_label"
    ] = top_skills_per_profession_and_nation_df.esco_id.map(skill2name_mapper)

    # create flag for whether the skill is in the top 10 EYP skills for the profession
    top_skills_per_profession_and_nation_df[
        "in_eyp_top_skills"
    ] = top_skills_per_profession_and_nation_df.apply(
        lambda x: True
        if x.esco_label
        in top_skills_per_profession_and_nation_df[
            top_skills_per_profession_and_nation_df.profession
            == "Early Years Practitioner"
        ].esco_label.unique()
        else False,
        axis=1,
    )
    top_skills_per_profession_and_nation_df.to_csv(
        analysis_data_path / "top_skills_per_profession_and_nation.csv", index=False
    )

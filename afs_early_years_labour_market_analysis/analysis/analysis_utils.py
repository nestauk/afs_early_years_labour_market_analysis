"""
Functions to:
    - generate Nesta branch compliant generation of graphs
    - load analysis data
    - generate altair graphs for OJO analysis and streamlit app
"""
import altair as alt
import pandas as pd
from colour import Color

from typing import Tuple

NESTA_COLOURS = [
    "#0000FF",
    "#FDB633",
    "#18A48C",
    "#9A1BBE",
    "#EB003B",
    "#FF6E47",
    "#646363",
    "#0F294A",
    "#97D9E3",
    "#A59BEE",
    "#F6A4B7",
    "#D2C9C0",
    "#000000",
]

salary_mapper = {"inflation_adj_min_salary": "Min", "inflation_adj_max_salary": "Max"}

clean_salary_mapper = {
    "Median Minimum Annualised Salary (£)": "Min",
    "Median Maximum Annualised Salary (£)": "Max",
}


def nestafont(font: str = "Century Gothic"):
    """Define Nesta fonts"""
    return {
        "config": {
            "title": {"font": font, "anchor": "start"},
            "axis": {"labelFont": font, "titleFont": font},
            "header": {"labelFont": font, "titleFont": font},
            "legend": {"labelFont": font, "titleFont": font},
            "range": {
                "category": NESTA_COLOURS,
                "ordinal": {
                    "scheme": NESTA_COLOURS
                },  # this will interpolate the colors
            },
        }
    }


alt.themes.register("nestafont", nestafont)
alt.themes.enable("nestafont")


def configure_plots(
    fig,
    font: str = "Century Gothic",
    chart_title: str = "",
    chart_subtitle: str = "",
    fontsize_title: int = 16,
    fontsize_subtitle: int = 13,
    fontsize_normal: int = 13,
):
    """Adds titles, subtitles; configures font sizes; adjusts gridlines"""
    return (
        fig.properties(
            title={
                "anchor": "start",
                "text": chart_title,
                "fontSize": fontsize_title,
                "subtitle": chart_subtitle,
                "subtitleFont": font,
                "subtitleFontSize": fontsize_subtitle,
            },
        )
        .configure_axis(
            gridDash=[1, 7],
            gridColor="grey",
            labelFontSize=fontsize_normal,
            titleFontSize=fontsize_normal,
        )
        .configure_legend(
            titleFontSize=fontsize_title,
            labelFontSize=fontsize_normal,
        )
        .configure_view(strokeWidth=0)
    )


def load_analysis_data(analysis_path: str) -> Tuple[pd.DataFrame]:
    """Load analysis data from path.

    Args:
        analysis_path (str): path to analysis directory

    Returns:
        Tuple[pd.DataFrame]: tuple of dataframes to be used for analysis
    """
    monthly_profession_mean = pd.read_csv(
        f"{analysis_path}/monthly_profession_mean.csv"
    )
    monthly_profession_sal_count = pd.read_csv(
        f"{analysis_path}/monthly_profession_sal_count.csv"
    )
    qualification_level_salary_boxplot = pd.read_csv(
        f"{analysis_path}/qualification_level_salary_boxplot.csv"
    )
    salary_by_profession_boxplot = pd.read_csv(
        f"{analysis_path}/salary_by_profession_boxplot.csv"
    )
    skill_profile_similarity = pd.read_csv(
        f"{analysis_path}/skill_profile_similarity.csv"
    )
    top_skills_per_profession_and_nation = pd.read_csv(
        f"{analysis_path}/top_skills_per_profession_and_nation.csv"
    )
    wage_ratio_df = pd.read_csv(f"{analysis_path}/wage_ratio_df.csv")
    median_salary_count_per_profession_table = pd.read_csv(
        f"{analysis_path}/median_salary_count_per_profession_table.csv"
    )
    median_salary_count_per_qual_table = pd.read_csv(
        f"{analysis_path}/median_salary_count_per_qual_table.csv"
    )

    return (
        monthly_profession_mean,
        monthly_profession_sal_count,
        qualification_level_salary_boxplot,
        salary_by_profession_boxplot,
        skill_profile_similarity,
        top_skills_per_profession_and_nation,
        wage_ratio_df,
        median_salary_count_per_profession_table,
        median_salary_count_per_qual_table,
    )


def generate_boxplot(
    box_plot_df: pd.DataFrame, facet_type: str, columns: int
) -> alt.Chart:
    """Generate boxplot of salary by different categorical variables

    Args:
        box_plot_df (pd.DataFrame): DataFrame to generate boxplot from
        facet_type (str): Type of facet to generate boxplot by
        columns (int): Number of columns to facet by
        streamlit (bool, optional): Whether to generate altair chart for streamlit app. Defaults to False.

    Returns:
        alt.Chart: altair chart of boxplot
    """
    return (
        alt.Chart(box_plot_df)
        .mark_boxplot(size=50, extent=0.5, outliers=False)
        .encode(
            alt.Y(
                "salary:Q", title="Annualised Salary (£)", scale=alt.Scale(zero=False)
            ),
            alt.X("salary_type:N", title=""),
            alt.Color("salary_type:N", title="Salary Type"),
        )
        .properties(width=175, height=200)
        .facet(facet_type, columns=columns)
    )


def generate_wage_ratio(wage_ratio_df: pd.DataFrame) -> alt.Chart:
    """Generate chart of wage ratio by qualification level

    Args:
        wage_ratio_df (pd.DataFrame): DataFrame to generate chart from

    Returns:
        alt.Chart: Chart of wage ratio by qualification level
    """
    qual_sorted = ["6", "3", "2"]

    qual_count = (
        alt.Chart(wage_ratio_df)
        .mark_bar()
        .encode(
            # sort values by count in descending order
            x=alt.X("count:Q", title="# of Job Adverts"),
            y=alt.Y(
                "qualification_level",
                title="Qualification Level",
                sort=qual_sorted,
                type="ordinal",
            ),
            color=alt.Color("requires_degree", title="Requires Degree"),
        )
        .properties(width=275, height=150)
    )

    qual_wage_ratio = (
        alt.Chart(wage_ratio_df)
        .mark_bar()
        .encode(
            # sort values by count in descending order
            x=alt.X("max_wage_ratio:Q", title="Wage Ratio"),
            y=alt.Y(
                "qualification_level",
                title="Qualification Level",
                sort=qual_sorted,
                axis=None,
                type="ordinal",
            ),
            color=alt.Color("requires_degree", title="Requires Degree"),
        )
        .properties(width=275, height=150)
    )

    wage_line = (
        alt.Chart(pd.DataFrame({"x": [1]}))
        .mark_rule(color="red")
        .encode(x="x")
        .properties(width=275, height=150)
    )

    qual_wage_ratio_line = qual_wage_ratio + wage_line

    qual_graph = qual_count | qual_wage_ratio_line

    return qual_graph


def generate_profession_ts_salary(
    monthly_profession_sal_count_melt: pd.DataFrame, streamlit: bool = False
) -> alt.Chart:
    """Generate time series of median monthly salary by profession

    Args:
        monthly_profession_sal_count_melt (pd.DataFrame): DataFrame to generate chart from
        streamlit (bool, optional): Whether to generate chart for streamlit. Defaults to False.

    Returns:
        alt.Chart: Time series of median monthly salary by profession
    """
    salary_ts = (
        alt.Chart(monthly_profession_sal_count_melt)
        .mark_line()
        .encode(
            x=alt.X("month_year:T", title="Date"),
            y=alt.Y("salary", title="Median Annualised Salary (£)"),
            color=alt.Color("salary_type", title="Salary Type"),
        )
    )

    # add pattern to median line
    median_wage_line = (
        alt.Chart(monthly_profession_sal_count_melt)
        .mark_rule(color="black", strokeDash=[1, 1])
        .encode(y="median_salary")
    )

    median_wage_profession = alt.layer(
        salary_ts, median_wage_line, data=monthly_profession_sal_count_melt
    )

    if streamlit:
        return median_wage_profession
    else:
        return median_wage_profession.facet("profession", columns=5)


def generate_profession_ts_count(
    monthly_mean: pd.DataFrame, streamlit: bool = False
) -> alt.Chart:
    """Generate time series of rolling monthly average of job adverts by profession

    Args:
        monthly_mean (pd.DataFrame): DataFrame to generate chart from
        streamlit (bool, optional): Whether to generate chart for streamlit. Defaults to False.s

    Returns:
        alt.Chart: Time series of rolling monthly average of job adverts by profession
    """
    ts_count = (
        alt.Chart(monthly_mean)
        .mark_line()
        .encode(
            x=alt.X("created:T", title="Date"),
            y=alt.Y(f"count:Q", title="Rolling Monthly Average"),
            color=alt.Color("profession", legend=None),
        )
    )

    if streamlit:
        return ts_count
    else:
        return ts_count.facet("profession", columns=5, title=None)


def generate_skill_similarity(skill_profile_sims: pd.DataFrame) -> alt.Chart:
    """Generate Similarity of EYP Skill Profile to Other professions Graph

    Args:
        skill_profile_sims (pd.DataFrame): DataFrame to generate chart from

    Returns:
        alt.Chart: Similarity of EYP Skill Profile to Other professions Graph
    """
    # Generate Similarity of EYP Skill Profile to Other professions Graph
    most_similar_color = Color("red")
    least_similar_color = Color("green")
    similarity_colors = {
        sim_value / 10: str(c.hex)
        for sim_value, c in enumerate(
            list(most_similar_color.range_to(least_similar_color, 10))
        )
    }

    similar_professions_text = pd.DataFrame(
        {"x": [0] * 4 + [1] * 4, "y": list(range(4, 0, -1)) + list(range(4, 0, -1))}
    )
    similar_professions_text = similar_professions_text[:-1]
    similar_professions_text_sim = pd.merge(
        skill_profile_sims, similar_professions_text, left_index=True, right_index=True
    )

    similar_professions_text_sim["skill_profile_similarity_approx"] = round(
        similar_professions_text_sim.skill_profile_similarity, 1
    )
    similar_professions_text_sim[
        "color"
    ] = similar_professions_text_sim.skill_profile_similarity_approx.map(
        similarity_colors
    )

    similar_professions_text_sim = similar_professions_text_sim.sort_values(
        "skill_profile_similarity", ascending=False
    )
    base = (
        alt.Chart(similar_professions_text_sim)
        .mark_circle()
        .encode(
            x=alt.X("x", axis=None),
            y=alt.Y("y", axis=None),
            size=alt.SizeValue(400),
            color=alt.Color("color", title="Similarity", scale=None),
        )
    )

    text = base.mark_text(
        align="left", dx=15, dy=0, font="Century Gothic", fontWeight="bold", yOffset=-5
    ).encode(
        x="x",
        y="y",
        # alt size value of 10
        size=alt.SizeValue(14),
        text="profession",
    )

    score = base.mark_text(
        align="left", dx=15, dy=0, font="Century Gothic", fontStyle="italic", yOffset=10
    ).encode(
        x="x",
        y="y",
        # alt size value of 10
        size=alt.SizeValue(12),
        text="skill_profile_similarity",
    )

    similar_professions_colors = pd.DataFrame(
        {
            "x": [0, 0, 0, 0],
            "y": [0, 0, 0, 0],
            "color": ["#008000", "#72aa00", "#d58e00", "#f00"],
            "Skill Profile Similarity": [
                "Very similar",
                "Quite similar",
                "Somewhat similar",
                "Not similar",
            ],
        }
    )

    legend_chart = (
        alt.Chart(similar_professions_colors)
        .mark_circle(size=0)
        .encode(
            x=alt.X("x", title="", axis=None),
            y=alt.Y("y", title="", axis=None),
            color=alt.Color(
                "Skill Profile Similarity",
                scale=alt.Scale(
                    domain=list(
                        dict(
                            zip(
                                similar_professions_colors["Skill Profile Similarity"],
                                similar_professions_colors["color"],
                            )
                        ).keys()
                    ),
                    range=list(
                        dict(
                            zip(
                                similar_professions_colors["Skill Profile Similarity"],
                                similar_professions_colors["color"],
                            )
                        ).values()
                    ),
                ),
                legend=alt.Legend(title=""),
            ),
        )
    ).properties(width=-1, height=100)

    base_text = base + text + score
    base_text_legend = base_text | legend_chart

    return base_text_legend


def generate_top_skills_barchart(df: pd.DataFrame, profession: str) -> alt.Chart:
    """Generate Top Skills Bar Chart

    Args:
        df (pd.DataFrame): DataFrame to generate chart from
        profession (str): Profession to generate chart for

    Returns:
        alt.Chart: Top Skills Bar Chart
    """
    bar_chart = (
        alt.Chart(
            df.query(f"profession == '{profession}'"),
            title=f"Top Skills for {profession}",
        )
        .mark_bar()
        .encode(
            y=alt.Y(
                "esco_label", title=None, sort=None, axis=alt.Axis(labelLimit=5000)
            ),
            x=alt.X("job_ad_percent", title="% of Job Adverts"),
            color=alt.Color(
                "in_eyp_top_skills",
                title="In EYP Top Skills?",
                scale=alt.Scale(
                    domain=[True, False],
                    range=[NESTA_COLOURS[0], NESTA_COLOURS[1]],
                ),
            ),
        )
    )

    return bar_chart

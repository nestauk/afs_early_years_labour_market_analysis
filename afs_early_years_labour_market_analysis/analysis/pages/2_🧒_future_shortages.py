import streamlit as st
import geopandas as gpd
import random

from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource
from bokeh.palettes import YlGnBu9

from bokeh.models import GeoJSONDataSource, ColorBar, LinearColorMapper, TapTool
from pathlib import Path

import os
import pandas as pd
import altair as alt
from app_utils import configure_plots

PROJECT_DIR = Path(__file__).resolve().parents[1]

with open(os.path.join(str(PROJECT_DIR), "style.css")) as css:
    st.markdown(f"<style>{css.read()}</style>", unsafe_allow_html=True)

# ========================================
# ---------- Load Data ------------

st.markdown(
    '<span style="font-size:38px"><b>Early Years Staff - Future Shortfall</b></span>',
    unsafe_allow_html=True,
)

st.markdown(
    '<span style="font-size:24px"><b>Introduction</b></span>', unsafe_allow_html=True
)

st.markdown(
    """The early years workforce is a critical part of the UK economy...policy introduction..."""
)

st.markdown(
    '<span style="font-size:24px"><b>Methodology</b></span>', unsafe_allow_html=True
)

st.markdown(
    """We want to understand the impact on the early years workforce given the introduction of the new 30 hours free childcare policy.

We take a rules-based approach to calculate the number of additional staff that will need to enter the workforce to accomodate the increase in demand for childcare.

We use data from X, Y, Z data sources and

We make the following assumptions: x, y and z.

We calculate staff shortages based on two uptake scenarios: <b>high, medium, low</b>
""",
    unsafe_allow_html=True,
)

# ========================================
# ---------- Load data ------------

# load shapefile
la_data = gpd.read_file(
    "s3://afs-early-years-labour-market-analysis/inputs/la_geographies/Local_Authority_Districts_May_2022_UK_Shapefiles/LAD_MAY_2022_UK_BFE_V3.shp"
)
# load matplotlib colormap
la_data_len = len(la_data)
la_data["age"] = [[0, 1, 2, 3, 4] for _ in range(la_data_len)]
la_data = la_data.explode("age").reset_index(drop=True)
la_data_len = len(la_data)
la_data["low_staff_shortage"] = [
    random.choice(range(0, 10000)) for _ in range(la_data_len)
]
la_data["medium_staff_shortage"] = [
    random.choice(range(0, 10000)) for _ in range(la_data_len)
]
la_data["high_staff_shortage"] = [
    random.choice(range(0, 10000)) for _ in range(la_data_len)
]

## add other random attributes that we might want to map
# add number of children per age
for age in [0, 1, 2, 3, 4]:
    la_data[f"num_{str(age)}"] = [
        random.choice(range(0, 10000)) for _ in range(la_data_len)
    ]

# add IMD score
la_data["IMD_score"] = [random.choice(range(0, 10000)) for _ in range(la_data_len)]

# add urban/rural classification
la_data["urban_rural"] = [random.choice(["urban", "rural"]) for _ in range(la_data_len)]

# ========================================
# ---------- Summary Statistics ------------

st.markdown(
    "To get a sense of the scale of the problem, we can look at the number of additional staff needed by local authority and by age. Choose an age and a scenario type to see the number of additional staff needed overall."
)

col1, col2 = st.columns([30, 30])
with col1:
    age = st.selectbox("Select the age", ("0", "1", "2", "3", "4"))
with col2:
    scenario = st.selectbox("Select the scenario", ("high", "medium", "low"))

la_data_age = la_data[la_data["age"] == int(age)]

la_data_age_no_geo = la_data_age.drop(columns=["geometry"])

metric1, metric2 = st.columns((1, 1))

metric1.metric(
    label=f"*Number of {age} year olds*",
    value=la_data_age_no_geo[f"num_{str(age)}"].sum(),
)

metric2.metric(
    label=f"*Number of additional staff needed for {age} year olds*",
    value=f"{la_data_age_no_geo[f'{scenario}_staff_shortage'].sum()}",
)

# ========================================
# ---------- map configs ------------

st.markdown(
    '<span style="font-size:24px"><b>A Map of Staff Shortages</b></span>',
    unsafe_allow_html=True,
)

st.markdown(
    "To interrogate the local distrubtion of staff shortages, we can look at the map below. Hover over or click a local authority to get a sense of staff shortage and additional metadata including urban/rural classification and IMD score."
)
# Map settings - bring this into utils eventuall
map_color_bar_palette = YlGnBu9[::-1][2:]
vals = la_data_age_no_geo[f"{scenario}_staff_shortage"]

color_map = LinearColorMapper(
    palette=map_color_bar_palette, low=vals.min(), high=vals.max()
)

color_bar = ColorBar(
    color_mapper=color_map,
    label_standoff=8,
    width=340,
    height=20,
    location=(0, 0),
    orientation="horizontal",
    title="Staff Shortage Scale",
    title_text_font_size="12px",
    title_text_font_style="bold",
    title_text_font="Century Gothic",
    title_text_color="#666666",
)
map_plot_width = 400
map_tools = "wheel_zoom, pan, reset, hover, save"

# map_tooltips = [
#     ("Local authority", "@{}".format('LAD22NM')),
#     #(f"Number of {age} year olds", "@age"),
#     ("Staff shortage", f"@{scenario}_staff_shortage"),
#     (f"Number of {age} year olds", f"@num_{age}"),
#     ("IMD score", "@IMD_score"),
#     ("Urban/Rural classification", "@urban_rural"),
# ]

tooltips = (
    '<p style="font-family: Century Gothic"><b>Local authority:</b> @LAD22NM </p>',
    f'<p style="font-family: Century Gothic"><b>Staff shortage:</b> @{scenario}_staff_shortage </p>',
    f'<p style="font-family: Century Gothic"><b>Number of {age} year olds:</b> @num_{age} </p>',
    '<p style="font-family: Century Gothic"><b>IMD score: </b> @IMD_score </p>',
    '<p style="font-family: Century Gothic"><b>Urban/Rural classification: </b> @urban_rural </p>',
)

title = (
    f"Local Authority Shortages for {age} year olds under a {scenario} uptake scenario"
)
non_sel_color = "lightsteelblue"
non_sel_fill_color = "gray"

# ========================================
# -------- Interactive bokeh map ---------

# dump la_data_age to jsoon
source = GeoJSONDataSource(geojson=la_data_age.to_json())

# instantiate map plot
map_plot = figure(
    title=title,
    tools=map_tools,
    x_axis_location=None,
    y_axis_location=None,
    tooltips=" ".join(tooltips),
    x_axis_type="mercator",
    y_axis_type="mercator",
    plot_width=map_plot_width,
    match_aspect=True,
)
map_plot.add_layout(color_bar, "below")
map_plot.grid.grid_line_color = None
map_plot.hover.point_policy = "follow_mouse"
map_plot.match_aspect = True
map_plot.title.text_font = "Century Gothic"
map_plot.title.text_font_size = "20px"
map_plot.title.text_font_style = "bold"
# add title to the center
map_plot.title.align = "center"
map_plot.title.text_color = "#666666"

# Selection of local authorities by clicking on map

patch_renderer = map_plot.patches(
    "xs",
    "ys",
    source=source,
    fill_color={"field": f"{scenario}_staff_shortage", "transform": color_map},
    line_color=non_sel_color,
    line_width=0.5,
    fill_alpha=0.7,
    nonselection_line_color=non_sel_color,
    nonselection_line_alpha=0.8,
    nonselection_fill_color=non_sel_fill_color,
)

tap_tool = TapTool(renderers=[patch_renderer])
map_plot.add_tools(tap_tool)

# add patch
map_plot.patches(
    "xs",
    "ys",
    source=source,
    fill_alpha=1,
    line_width=0.5,
    line_color="black",
    fill_color={"field": "staff_shortage", "transform": color_map},
)

# #Display figure in streamlit
st.bokeh_chart(map_plot, use_container_width=True)

# ========================================
## ----- Other factors influencing staff shortages -----

st.markdown(
    '<span style="font-size:24px"><b>Other Factors influencing Staff Shortages</b></span>',
    unsafe_allow_html=True,
)

factors_text = f"Other factors can influence staff shortages, including the rural-urban divide or levels of deprivation in a local area. The correlation heat map below shows the correlation between staff shortages and other factors."

st.markdown(factors_text)

# correlation between additional factor and staff shortages
corrMatrix = (
    la_data_age_no_geo[[f"{scenario}_staff_shortage", "IMD_score", f"num_{age}"]]
    .rename(
        columns={
            f"{scenario}_staff_shortage": "Staff Shortage",
            "IMD_score": "IMD Score",
            f"num_{age}": f"Children aged {age}",
        }
    )
    .corr()
    .reset_index()
    .melt("index")
)
corrMatrix.columns = ["var1", "var2", "correlation"]

base = (
    alt.Chart(corrMatrix)
    .transform_filter(alt.datum.var1 < alt.datum.var2)
    .encode(
        x=alt.X("var1", title=None),
        y=alt.Y("var2", title=None),
    )
    .properties(width=alt.Step(100), height=alt.Step(100))
)

rects = base.mark_rect().encode(color="correlation")

text = base.mark_text(size=30).encode(
    text=alt.Text("correlation", format=".2f"),
    color=alt.condition(
        "datum.correlation > 0.5", alt.value("white"), alt.value("black")
    ),
)

corr_plot = configure_plots(rects + text)

st.altair_chart(
    corr_plot.configure_axis(labelLimit=300),
    use_container_width=True,
)

factors_detail_text = "To investigate additional factors more, select one from the drop down below to investigate distribution skewness of staff shortages for that factor."

st.markdown(factors_detail_text)

additional_factors_dict = {
    "Rural/Urban divide": "urban_rural",
    "IMD score": "IMD_score",
}

additional_factor = st.selectbox(
    "Select other factors to investigate", ("Rural/Urban divide", "IMD score")
)


# distribution of staff shortages based on additional factor

dist_factor_text = f"The graph below shows the distribution of staff shortages for {age} year olds under a {scenario} uptake scenario for {additional_factor}. Distibution 'skewedness' to the right indicates that there are more local authorities with higher staff shortages."

st.markdown(dist_factor_text)


factor_df = la_data_age_no_geo[
    [
        "LAD22NM",
        f"{scenario}_staff_shortage",
        additional_factors_dict.get(additional_factor),
    ]
]

base = (
    alt.Chart(factor_df)
    .mark_bar(opacity=0.3, binSpacing=0)
    .encode(
        alt.X(
            f"{scenario}_staff_shortage",
            bin=alt.Bin(maxbins=10),
            title="Staff Shortage",
        ),
        alt.Y("count()", title=None, axis=alt.Axis(labels=False, ticks=True)),
        alt.Color(
            additional_factors_dict.get(additional_factor), title=additional_factor
        ),
    )
)

vline = (
    alt.Chart(
        pd.DataFrame(
            {
                "average_staff_shortage": la_data_age_no_geo[
                    f"{scenario}_staff_shortage"
                ].mean(),
                "color": ["red"],
            }
        )
    )
    .mark_rule(opacity=0.8)
    .encode(x="average_staff_shortage", color=alt.Color("color:N", scale=None))
)

base_line = base + vline

base_line_configured = configure_plots(
    base_line,
    chart_title=[
        f"Staff shortage distribution for {age} year olds",
        f"under a '{scenario} uptake' scenario",
    ],
    fontsize_title=18,
    fontsize_subtitle=14,
)

st.altair_chart(
    base_line_configured.configure_axis(labelLimit=300),
    use_container_width=True,
)

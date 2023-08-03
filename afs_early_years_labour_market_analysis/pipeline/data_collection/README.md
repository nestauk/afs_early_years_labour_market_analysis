# Data Collection

This folder contains the scripts used to collect the data used in the analysis.

## Data Sources

The data used in the analysis is sourced from the following sources:

1. Open Jobs Observatory

## Pipelines

1. `refine_relevant_jobs.py` - looks for relevant jobs in the OJO data. To run, execute the following command from this directory:
   `bash python refine_relevant_jobs.py run `

Similar job titles are from `s3://afs-early-years-labour-market-analysis/inputs/similar_occupations.txt`. These job titles are determined by using [Karlis Kanders' Career Transitions algorithm](https://github.com/nestauk/mapping-career-causeways). Job titles that have a similar score of at least 0.6 to 'early years teacher' are included in the `similar_occupations.txt` file.

We use this list as a starting point to manually identify job titles that are relevant to early years teachers. We also manually add job titles related to retail and hospitality.

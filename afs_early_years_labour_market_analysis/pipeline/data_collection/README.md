# Data Collection

This folder contains the scripts used to collect the data used in the analysis.

## Data Sources

The data used in the analysis is sourced from the following sources:

1. Open Jobs Observatory

## Pipelines

1. `refine_relevant_jobs.py` - looks for relevant jobs in the OJO data. To run, execute the following command from this directory:
   `bash python refine_relevant_jobs.py run `
   Similar job titles are from `s3://afs-early-years-labour-market-analysis/inputs/similar_occupations.txt`. These job titles are determined by using [Karlis Kander's Career Transitions algorithm](https://github.com/nestauk/mapping-career-causeways). Job titles that have a similar score of at least 0.6 to 'early years teacher' are included in the `similar_occupations.txt` file. Additional job titles are manually added to flesh out the initial 'seed' list. Finally, job titles related to `sales assistants` are also added, based on expert advice that there are many transitions from sales assistants to early years teachers.

2. `enrich_relevant_jobs.py` - adds location and salary to job adverts. Also adds a dataset of skills extracted from the relevant job adverts. To run, execute the following command from this directory:
   ```bash
   python enrich_relevant_jobs.py run
   ```

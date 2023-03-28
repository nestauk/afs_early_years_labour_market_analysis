# Data Collection

This folder contains the scripts used to collect the data used in the analysis.

## Data Sources

The data used in the analysis is sourced from the following sources:
1. Open Jobs Observatory

## Pipelines

1. `refine_relevant_jobs.py` - looks for relevant jobs in the OJO data. To run, execute the following command from this directory:
    ```bash
    python refine_relevant_jobs.py run
    ```
2. `refine_relevant_jobs.py` - adds location and salary to job adverts. Also adds a dataset of skills extracted from the relevant job adverts. To run, execute the following command from this directory:
    ```bash
    python refine_relevant_jobs.py run
    ```
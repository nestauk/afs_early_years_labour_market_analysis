# Analysis of Early Years Job Advertisement Postings

This repo contains the scripts needed to replicate analysis of job postings for Early Years Practitioners.

To **filter and enrich** job adverts from the Open Jobs Observatory, refer to code in `pipeline/data_collection` and `pipeline/data_enrichement`.

To **analyse** enriched, filtered data, refer to the notebook in `notebooks/`.

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `direnv` and `conda`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure `pre-commit`
- Download spacy model: `python -m spacy download en_core_web_sm`

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>

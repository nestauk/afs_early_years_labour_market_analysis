## 📊 OJO Analysis

This directory contains the jupyter notebook that analysis both Early Years Practitioner and similar occupations job advert data from the Open Jobs Observatory.

It contains:

### 1. **`/ojo_analysis_utils.py`**

Variables and functions to aid in the analysis of job postings.

### 2. **`/ojo_analysis_notebook.py`**

This .py file is an exported notebook used for analysis. It is best to review it as a `.ipynb` file.

The notebook is split into sections related to:

- 1. Data loading and cleaning;
- 2. Generating tables;
- 3. Generating graphs
  - 3.1 Graphs related to salaries;
  - 3.2 Graphs related to count of job adverts;
  - 3.3 Graphs related to percent change on demand;
  - 3.4 Graphs related to skills

To review it as a notebook:

```
pip install jupytext #install the converter
jupytext --set-formats py,ipynb afs_early_years_labour_market_analysis/notebooks/ojo_analysis_notebook.py
```

### 3. **`images/`:**

Graphs from the OJO analysis notebook saved in `.png` format.

# FBI CDE Data Utility

This repo develops utilities for collecting and exploring FBI NIBRS/UCR data, parsing said data both from the FBI Crime Data Explorer API and from the FBI NIBRS master datasets.

API data collection will be implemented first, and the master dataset parser will be implemented second and used to validate the API data.

## Setup

I'm using [uv](https://docs.astral.sh/uv/) for this project, but the core non-dev dependencies are pretty standard and mature (python built-ins, `pandas`/`geopandas`, `requests`, `duckdb`). If you have `uv` [installed](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer), you can recreate my env by running `uv sync` (after cloning the repo and `cd`ing into it), and it will create an env using the dependencies specified in the `pyproject.toml`.

This project requires an FBI CDE API key to work. You can get one for free by entering your name and email into this [request form](https://api.data.gov/signup/) and then you'll immediately receive an API key. This project expects this key to either be in an environment variable named `CDE_API_KEY` or in a `.env` file with key-name `CDE_API_KEY`.

## Usage

At present, this project collects ORI data for the states listed [here] in the `<project_root>/src/elt.py` file and ingests it into a DuckDB database file (at `<project_root>/data/databases/cde_dwh.duckdb`).

To run the pipeline from the command line, activate your env and run the `<project_root>/src/elt.py` script.

```console
source .venv/bin/activate. # or .venv\Scripts\activate on windows
python src/elt.py
```

### Query interface

Run this command to open the Streamlit database querying interface. The console output will include three URLs, click the one that's appropriate for your situation (or try all three if you're unsure).

```console
streamlit run apps/query_interface.py
```


# Source Data

## NIBRS API

The NIBRS API is documented [here](https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi).

To download data from the FBI CDE API, you need an API key. The process for getting one takes about a minute and is described above in the **Setup** section.

## NIBRS Master Extracts

The NIBRS master extracts can be downloaded from the Crime Data Explorer [Documents and Downloads](https://cde.ucr.cjis.gov/LATEST/webapp/#) page. These files must be downloaded manually (the site's `robots.txt` is just `{"message":"Forbidden"}` and the site clearly discourages programmatic downloading) and this project expects the the NIBRS master files to be in the `PROJECT_ROOT/data/master/` directory with the default file_names (e.g., `nibrs-2023.zip`).

To download these files:
1. Go to the **Master File Downloads** section of the [CDE D&D  page](https://cde.ucr.cjis.gov/LATEST/webapp/#)
    1.a. Select the **National Incident-Based Reporting System (NIBRS)** option from the first dropdown.
    1.b. Select the desired year from the second drop down.
    1.c. Click **DOWNLOAD**.
2. Refresh the page and repeat this process until all desired years are downloaded.
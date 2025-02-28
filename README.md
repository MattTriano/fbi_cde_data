# FBI CDE Data Platform

This repo develops utilities for collecting and exploring FBI NIBRS/UCR data, parsing said data both from the FBI Crime Data Explorer (CDE) API and from the FBI NIBRS master datasets.

At present, this project implements:
* a pipeline to ingest FBI CDE datasets from master archive files into a DuckDB database,
* a pipeline to ingest some data from CDE API endpoints into that DuckDB database, and
* a (streamlit) web app that allows you to query the database from your browser.

## Setup

I'm using [uv](https://docs.astral.sh/uv/) for this project, but the core non-dev dependencies are pretty standard and mature (python built-ins, `pandas`/`geopandas`, `requests`, `duckdb`). If you have `uv` [installed](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer), you can recreate my env by running `uv sync` (after cloning the repo and `cd`ing into it), and it will create an env using the dependencies specified in the `pyproject.toml`.

This project requires an FBI CDE API key to work. You can get one for free by entering your name and email into this [request form](https://api.data.gov/signup/) and then you'll immediately receive an API key. This project expects this key to either be in an environment variable named `CDE_API_KEY` or in a `.env` file with key-name `CDE_API_KEY`.

## Usage

At present, this project primarily runs an ingestion pipeline that ingests NIBRS master data files in the `/data/master/` directory (these files must have the default name pattern used by the FBI `nibrs-<yyyy>.zip`). These data files must be manually downloaded from the FBI CDE site as described in the [NIBRS Master Extracts](#nibrs-master-extracts) section below.


To run the pipeline from the command line, run the `<project_root>/src/elt.py` script.

```console
uv run python src/elt.py
```

You can also run the API pipeline by adding on the `--run_api_pipeline` flag. At present, this pipeline collects ORI data for the states listed [here](https://github.com/MattTriano/fbi_cde_data/blob/cd21ad0c24124adb874515a5bdf94a3c133c535e/src/parsers/constants.py#L1) in the `<project_root>/src/parsers/constants.py` file and ingests it into a DuckDB database file (at `<project_root>/data/databases/cde_dwh.duckdb`).

```console
uv run python src/elt.py --run_api_pipeline
```

### dbt commands

To run dbt tests:
```console
uv run dbt test
```

To compile and materialize views or tables into the prod database:
```console
uv run dbt run --target prod
```
Note: this pipeline currently doesn't create a dev database as raw nibrs data alone is 20GB and this isn't a prod system yet.

### Query interface

Run this command to open the Streamlit database querying interface. The console output will include three URLs, click the one that's appropriate for your situation (or try all three if you're unsure).

```console
uv run streamlit run apps/query_interface.py
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
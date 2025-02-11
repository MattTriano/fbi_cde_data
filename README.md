# FBI CDE Data Utility

This repo develops utilities for collecting and exploring FBI NIBRS/UCR data, parsing said data both from the FBI Crime Data Explorer API and from the FBI NIBRS master datasets.

API data collection will be implemented first, and the master dataset parser will be implemented second and used to validate the API data.

## Usage

I'm using [uv](https://docs.astral.sh/uv/) for this project, but the core non-dev dependencies are pretty standard and mature (python built-ins, `pandas`/`geopandas`, `requests`, `pyarrow`, `duckdb`). If you have `uv` installed, you can recreate my env by running `uv sync`, and it will create an env using the dependencies specified in the `pyproject.toml`.

# Source Data

## NIBRS API

The NIBRS API is documented [here](https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/docApi).

To download data from the FBI CDE API, you need an API key. You can get one for free by entering your name and email into this [request form](https://api.data.gov/signup/) and then you'll immediately receive an API key. This project expects this key to either be in an environment variable named `CDE_API_KEY` or in a `.env` file with key-name `CDE_API_KEY`.

## NIBRS Master Extracts

The NIBRS master extracts can be downloaded from the Crime Data Explorer [Documents and Downloads](https://cde.ucr.cjis.gov/LATEST/webapp/#) page. These files must be downloaded manually (the site's `robots.txt` is just `{"message":"Forbidden"}` and the site clearly discourages programmatic downloading) and this project expects the the NIBRS master files to be in the `PROJECT_ROOT/data/master/` directory with the default file_names (e.g., `nibrs-2023.zip`).

To download these files:
1. Go to the **Master File Downloads** section of the [CDE D&D  page](https://cde.ucr.cjis.gov/LATEST/webapp/#)
    1.a. Select the **National Incident-Based Reporting System (NIBRS)** option from the first dropdown.
    1.b. Select the desired year from the second drop down.
    1.c. Click **DOWNLOAD**.
2. Refresh the page and repeat this process until all desired years are downloaded.
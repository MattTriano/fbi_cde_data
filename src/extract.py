import os
from pathlib import Path
import re
from typing import Optional
import warnings

import pandas as pd
import requests


def read_dotenv_file(file_path: Path) -> dict[str, str]:
    if not file_path.is_file():
        raise FileNotFoundError()
    with open(file_path, "r") as f:
        creds = f.readlines()
        cred_dict = {el[0]: el[1] for el in [c.replace("\n", "").split("=") for c in creds]}
    return cred_dict


class CDEAPI:
    base_url = "https://api.usa.gov/crime/fbi/cde"

    def __init__(self, dotenv_path: Optional[Path] = None):
        self._set_api_key(dotenv_path)

    def _set_api_key(self, dotenv_path: Optional[Path], api_key_key: str = "CDE_API_KEY") -> None:
        if dotenv_path.is_file():
            cred_dict = read_dotenv_file(dotenv_path)
            api_key = cred_dict.get(api_key_key, None)
        else:
            api_key = os.environ.get(api_key_key, None)
        if api_key is not None:
            self.api_key = api_key
        else:
            raise KeyError(
                f"No FBI API key (with key-name {api_key_key}) found in .env file or in env-vars"
            )

    def get(self, api_call: str) -> dict:
        if "?" in api_call:
            join_char = "&"
        else:
            join_char = "?"
        formatted_api_call = f"{api_call}{join_char}API_KEY={self.api_key}"
        resp = requests.get(formatted_api_call)
        resp.raise_for_status()
        return resp.json()

    def _add_retrieved_at_time(self, df: pd.DataFrame) -> pd.DataFrame:
        retrieved_at = pd.Timestamp.now(tz="utc").floor("s")
        df["retrieved_at"] = retrieved_at
        return df

    def get_state_oris(self, state: str) -> pd.DataFrame:
        api_call = f"{self.base_url}/agency/byStateAbbr/{state.upper()}"
        result = self.get(api_call)
        county_oris = []
        for county, oris in result.items():
            county_df = pd.json_normalize(oris)
            county_df["county_key"] = county
            county_oris.append(county_df)
        with warnings.catch_warnings():
            # TODO: pandas 2.1.0 has a FutureWarning for concatenating DataFrames with Null entries
            warnings.filterwarnings("ignore", category=FutureWarning)
            ori_df = pd.concat([df for df in county_oris if not df.empty], ignore_index=True)
        return self._add_retrieved_at_time(ori_df)

    def _check_to_from_str(self, to_from: str) -> None:
        pattern = re.compile(r"^(([0]\d)|([1][0-2]))-((199[1-9])|(20[0-3]\d))$")
        if not re.match(pattern, to_from):
            raise ValueError(
                "Incorrect format. Resubmit the given month-year string in mm-yyyy format."
            )
        else:
            today = pd.Timestamp.now(tz="utc").floor("D")
            year = int(to_from[3:])
            month = int(to_from[:2])
            if year > today.year or (year == today.year and month >= today.month):
                raise ValueError(
                    "Future month-year string given. Data from the future not yet available."
                )

    def _fmt_nibrs_agency_ori_offense_call(
        self, ori: str, type: str, from_date: str, to_date: str, offense: str
    ) -> str:
        valid_types = ("counts", "totals")
        valid_offenses = ("all", "11")
        if type not in valid_types:
            raise ValueError(
                "Received an invalid 'type' value for this API endpoint. "
                f"Valid values: {valid_types}"
            )
        if offense not in valid_offenses:
            raise ValueError(
                "Received an invalid 'offense' value for this API endpoint. "
                f"Valid values: {valid_offenses}"
            )
        self._check_to_from_str(from_date)
        self._check_to_from_str(to_date)
        if pd.to_datetime(from_date) > pd.to_datetime(to_date):
            raise ValueError("The `to` date cannot predate the 'from' date.")
        api_call = (
            f"{self.base_url}/nibrs/agency/{{ori}}/{offense}?type={type}&"
            f"from={from_date}&to={to_date}&ori={ori}"
        )
        return api_call

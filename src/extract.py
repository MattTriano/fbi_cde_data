import os
from pathlib import Path
from typing import Optional

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
        formatted_api_call = f"{api_call}?API_KEY={self.api_key}"
        resp = requests.get(formatted_api_call)
        resp.raise_for_status()
        return resp.json()

    def get_state_oris(self, state: str) -> pd.DataFrame:
        api_call = f"{self.base_url}/agency/byStateAbbr/{state.upper()}"
        result = self.get(api_call)
        county_oris = []
        for county, oris in result.items():
            county_df = pd.json_normalize(oris)
            county_df["county_key"] = county
            county_oris.append(county_df)
        return pd.concat(county_oris, ignore_index=True)

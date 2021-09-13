from typing import List

from pyriksprot.utility import download_url

GITHUB_SOURCE_URL = "https://github.com/welfare-state-analytics/riksdagen-corpus/raw/main/corpus"


def download_parla_clarin_protocols(protocols: List[str], target_folder: str) -> None:
    for filename in protocols:
        sub_folder: str = filename.split('-')[1]
        url: str = f'{GITHUB_SOURCE_URL}/{sub_folder}/{filename}'
        download_url(url, target_folder, filename)

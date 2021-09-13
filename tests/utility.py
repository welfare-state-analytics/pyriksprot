from os.path import join as jj
from typing import List

from pyriksprot.utility import download_url

GITHUB_SOURCE_URL = "https://github.com/welfare-state-analytics/riksdagen-corpus/raw/main/corpus"

TEST_PROTOCOLS = [
    'prot-1936--ak--8.xml',
    'prot-1961--ak--5.xml',
    'prot-1961--fk--6.xml',
    'prot-198687--11.xml',
    'prot-200405--7.xml',
]

DEFAULT_ROOT_PATH = jj("tests", "test_data", "work_folder")


def download_parla_clarin_protocols(protocols: List[str], target_folder: str) -> None:
    for filename in protocols:
        sub_folder: str = filename.split('-')[1]
        url: str = f'{GITHUB_SOURCE_URL}/{sub_folder}/{filename}'
        download_url(url, target_folder, filename)

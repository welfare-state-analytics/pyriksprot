import os
import shutil
from os.path import join as jj
from typing import List

from loguru import logger

from pyriksprot.utility import download_url

DEFAULT_DOWNLOAD_BRANCH = "dev"
DEFAULT_TEST_DOCUMENT_NAMES = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
    'prot-199192--127.xml',
    'prot-199192--21.xml',
]


def github_source_url(branch: str = DEFAULT_DOWNLOAD_BRANCH):
    return f"https://github.com/welfare-state-analytics/riksdagen-corpus/raw/{branch}/corpus"


def members_of_parliament_url(branch: str = DEFAULT_DOWNLOAD_BRANCH) -> str:
    return f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{branch}/corpus/members_of_parliament.csv'


def download_parliamentary_protocols(
    protocols: List[str], target_folder: str, create_subfolder: bool = True, branch: str = DEFAULT_DOWNLOAD_BRANCH
) -> None:
    os.makedirs(target_folder, exist_ok=True)
    for filename in protocols:
        if not filename.endswith('.xml'):
            filename = f"{filename}.xml"
        subfolder: str = filename.split('-')[1]
        url: str = f'{github_source_url(branch)}/{subfolder}/{filename}'
        file_target_folder = target_folder if not create_subfolder else jj(target_folder, subfolder)
        download_url(url, file_target_folder, filename)


def download_members_of_parliament(target_folder: str, branch: str = DEFAULT_DOWNLOAD_BRANCH):
    download_url(members_of_parliament_url(branch), target_folder, 'members_of_parliament.csv')


def create_parlaclarin_corpus(
    protocols: List[str] = None, target_folder: str = 'tests/test_data/source', branch: str = DEFAULT_DOWNLOAD_BRANCH
) -> None:

    if protocols is None:
        protocols = DEFAULT_TEST_DOCUMENT_NAMES

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)
    logger.info("downloading members of parliament")
    download_members_of_parliament(target_folder, branch)
    logger.info(f"downloading test protocols: {', '.join(protocols)}")
    download_parliamentary_protocols(protocols, target_folder, create_subfolder=True, branch=branch)

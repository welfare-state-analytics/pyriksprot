import os
import shutil
from os.path import expanduser
from os.path import join as jj
from urllib.parse import quote as q
from urllib.request import urlretrieve

from loguru import logger

from ..utility import ensure_path, replace_extension


def _download_to_folder(*, url: str, target_folder: str, filename: str) -> None:
    """Download a file from a url and place it in `target_folder`"""
    target_filename: str = ensure_path(jj(expanduser(target_folder), filename))
    urlretrieve(url, target_filename)
    logger.info(f'downloaded: {filename}')


def _protocol_uri(filename: str, subfolder: str, tag: str) -> str:
    return f"https://github.com/welfare-state-analytics/riksdagen-corpus/raw/{q(tag)}/corpus/protocols/{q(subfolder)}/{q(filename)}"


def download_protocols(*, filenames: list[str], target_folder: str, create_subfolder: bool, tag: str) -> None:
    """Downloads protocols, only used in tests when subsetting corpus."""

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)

    logger.info(f"downloading protocols from branch {tag}.")

    for filename in filenames:
        protocol_year: str = filename.split('-')[1]
        target_name: str = replace_extension(filename, 'xml')
        _download_to_folder(
            url=_protocol_uri(filename=target_name, subfolder=protocol_year, tag=tag),
            target_folder=target_folder if not create_subfolder else jj(target_folder, protocol_year),
            filename=target_name,
        )

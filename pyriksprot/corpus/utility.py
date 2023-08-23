import glob
import os
import shutil
from os.path import expanduser
from os.path import join as jj
from urllib.parse import quote as q
from urllib.request import urlretrieve

from loguru import logger

from ..utility import ensure_folder, ensure_path, replace_extension, reset_folder


def _download_to_folder(*, url: str, target_folder: str, filename: str) -> None:
    """Download a file from a url and place it in `target_folder`"""
    target_filename: str = ensure_path(jj(expanduser(target_folder), filename))
    urlretrieve(url, target_filename)
    logger.info(f'downloaded: {filename}')


def _protocol_uri(filename: str, subfolder: str, tag: str) -> str:
    return f"https://github.com/welfare-state-analytics/riksdagen-corpus/raw/{q(tag)}/corpus/protocols/{q(subfolder)}/{q(filename)}"


def download_protocols(*, filenames: list[str], target_folder: str, create_subfolder: bool, tag: str) -> None:
    """Downloads protocols, used when subsetting corpus. Does not accept wildcards."""

    reset_folder(target_folder, force=True)

    logger.info(f"downloading protocols from branch {tag}.")

    for filename in filenames:
        if '*' in filename:
            raise ValueError(f"wildcards not allowed in filename: {filename}")

        protocol_year: str = filename.split('-')[1]
        target_name: str = replace_extension(filename, 'xml')
        _download_to_folder(
            url=_protocol_uri(filename=target_name, subfolder=protocol_year, tag=tag),
            target_folder=target_folder if not create_subfolder else jj(target_folder, protocol_year),
            filename=target_name,
        )


def copy_protocols(
    *, source_folder: str, filenames: list[str], target_folder: str, create_subfolder: bool = True
) -> None:
    """Downloads protocols, used when subsetting corpus from folder. Accepts wildcards."""

    if not os.path.isdir(source_folder):
        raise ValueError(f"source_folder {source_folder} is not a folder")

    protocol_source_folder: str = jj(source_folder, 'corpus', 'protocols')

    if not os.path.isdir(protocol_source_folder):
        raise ValueError(f"source_folder {protocol_source_folder} is not a riksprot repository folder")

    reset_folder(target_folder, force=True)

    logger.info(f"copying protocols from {source_folder}.")

    for pattern in filenames:
        for path in glob.glob(jj(protocol_source_folder, '**', pattern), recursive=True):
            document_name: str = os.path.basename(path)
            protocol_year: str = document_name.split('-')[1]
            target_name: str = replace_extension(document_name, 'xml')
            target_sub_folder: str = target_folder if not create_subfolder else jj(target_folder, protocol_year)
            ensure_folder(target_sub_folder)
            shutil.copy(path, jj(target_sub_folder, target_name))

import glob
import os
import re
import shutil
import xml.etree.ElementTree as ET
from os.path import abspath, basename, expanduser
from os.path import join as jj
from typing import Literal
from urllib.parse import quote as q
from urllib.request import urlretrieve

from loguru import logger

from .. import gitchen as gh
from ..utility import ensure_folder, ensure_path, replace_extension, reset_folder


def _extract_tei_corpus_filenames(tei_filename: str) -> list[str]:
    """Extracts filenames from a TEI Corpus file."""
    # root: ET.Element = ET.fromstring(source)
    tree: ET.ElementTree = ET.parse(tei_filename)
    root = tree.getroot()
    namespaces: dict[str, str] = {'xi': 'http://www.w3.org/2001/XInclude'}
    filenames: list[str] = [
        element.get('href') for element in root.findall('.//xi:include', namespaces) if element.get('href') is not None
    ]
    return filenames


def ls_corpus_by_tei_corpora(
    folder: str, mode: Literal['dict', 'filenames', 'tuples'] = 'dict', normalize: bool = True
) -> dict[str, dict[str, str | list[str]]] | list[str] | list[tuple[str, str]]:
    """Returns a list of XML documents as specified in teiCorpus files that reside in `folder`."""
    tei_corpora_filenames: list[str] = glob.glob(jj(folder, 'prot-*.xml'), recursive=False)
    data: dict[str, dict[str, str | list[str]]] = {}
    for tei_filename in tei_corpora_filenames:
        chamber: str = basename(tei_filename).split('-')[1].split('.')[0]
        if chamber not in ['ak', 'fk', 'ek']:
            raise ValueError(f"illegal chamber: {chamber}")
        filenames: str = [replace_extension(f, 'xml') for f in _extract_tei_corpus_filenames(tei_filename)]
        if normalize:
            filenames = [abspath(jj(folder, filename)) for filename in filenames]
        data[chamber] = {
            'chamber': chamber,
            'filenames': filenames,
        }
    if mode == 'dict':
        return data
    if mode == 'filenames':
        return [filename for item in data.values() for filename in item['filenames']]
    if mode == 'tuples':
        return [(item['chamber'], filename) for item in data.values() for filename in item['filenames']]
    return data


def ls_corpus_folder(folder: str, pattern: str = None) -> list[str]:
    """List all ParlaCLARIN files in a folder and it's subfolders."""

    if pattern is not None:
        if pattern.startswith('**/'):
            pattern = pattern[3:]
        return glob.glob(jj(folder, '**', pattern), recursive=True)

    regex_pattern: str = r'prot-\d*-.*-\d*\.xml'
    rx: re.Pattern[str] = re.compile(regex_pattern)
    candidates: list[str] = glob.glob(jj(folder, '**', 'prot-*.xml'), recursive=True)
    return [f for f in candidates if rx.match(basename(f))]


def _download_to_folder(*, url: str, target_folder: str, filename: str) -> None:
    """Download a file from a url and place it in `target_folder`"""
    target_filename: str = ensure_path(jj(expanduser(target_folder), filename))
    urlretrieve(url, target_filename)
    logger.info(f'downloaded: {filename}')


def _protocol_uri(filename: str, subfolder: str, tag: str, **opts) -> str:
    return gh.gh_create_url(
        user=opts.get("user"),
        repository=opts.get("repository"),
        path=f'{opts.get("path")}/{q(subfolder)}',
        filename=q(filename),
        tag=q(tag),
    )


def download_protocols(*, filenames: list[str], target_folder: str, create_subfolder: bool, tag: str, **opts) -> None:
    """Downloads protocols, used when subsetting corpus. Does not accept wildcards."""

    reset_folder(target_folder, force=True)

    logger.info(f"downloading protocols from branch {tag}.")

    for filename in filenames:
        if '*' in filename:
            raise ValueError(f"wildcards not allowed in filename: {filename}")

        protocol_year: str = filename.split('-')[1]
        target_name: str = replace_extension(filename, 'xml')
        _download_to_folder(
            url=_protocol_uri(filename=target_name, subfolder=protocol_year, tag=tag, **opts),
            target_folder=target_folder if not create_subfolder else jj(target_folder, protocol_year),
            filename=target_name,
        )


def copy_protocols(
    *, source_folder: str, filenames: list[str], target_folder: str, create_subfolder: bool = True
) -> None:
    """Downloads protocols, used when subsetting corpus from folder. Accepts wildcards."""

    if not os.path.isdir(source_folder):
        raise ValueError(f"source_folder {source_folder} is not a folder")

    reset_folder(target_folder, force=True)

    logger.info(f"copying protocols from {source_folder}.")

    for pattern in filenames:
        for path in glob.glob(jj(source_folder, '**', pattern), recursive=True):
            document_name: str = os.path.basename(path)
            protocol_year: str = document_name.split('-')[1]
            target_name: str = replace_extension(document_name, 'xml')
            target_sub_folder: str = target_folder if not create_subfolder else jj(target_folder, protocol_year)
            ensure_folder(target_sub_folder)
            shutil.copy(path, jj(target_sub_folder, target_name))

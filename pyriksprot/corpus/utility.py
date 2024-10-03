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

from jinja2 import Environment, FileSystemLoader, Template, TemplateNotFound
from loguru import logger

from .. import gitchen as gh
from .. import templates as tp
from ..utility import ensure_folder, ensure_path, replace_extension, reset_folder, strip_path_and_extension


def get_chamber_by_filename(filename: str) -> str:
    if '-fk-' in filename:
        return 'fk'
    if '-ak-' in filename:
        return 'ak'
    return 'ek'


def format_protocol_name(
    document_name: str, chamber_abbrev: Literal['ak', 'fk', 'ek'] = None, speech_nr: int | None = None
) -> str:
    try:
        if document_name is None:
            return ""

        if document_name.endswith(".xml"):
            document_name = document_name[:-4]

        chamber_names: dict[str, str] = {"fk": "Första kammaren", "ak": "Andra kammaren", "ek": ""}

        parts: list[str] = document_name.split("-")
        p_nr: str = parts[-1]
        s_nr: str = f"{speech_nr:03}" if speech_nr is not None else ""

        if '_' in p_nr:
            p_nr, s_nr = p_nr.split("_")

        year: str = parts[1]
        if len(year) == 6:
            year = f"{year[:4]}/{year[4:]}"
        elif len(year) == 8:
            year = f"{year[:4]}/{year[4:]}"

        if chamber_abbrev is None:
            chamber_abbrev = "fk" if "-fk-" in document_name else "ak" if "-ak-" in document_name else "ek"

        return f"{chamber_names[chamber_abbrev]} {year}:{p_nr.lstrip('0')} {s_nr}".strip()

    except IndexError:
        return document_name


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
        chamber_abbrev: str = basename(tei_filename).split('-')[1].split('.')[0]
        if chamber_abbrev not in ['ak', 'fk', 'ek']:
            raise ValueError(f"illegal chamber: {chamber_abbrev}")
        filenames: str = [replace_extension(f, 'xml') for f in _extract_tei_corpus_filenames(tei_filename)]
        if normalize:
            filenames = [abspath(jj(folder, filename)) for filename in filenames]
        data[chamber_abbrev] = {
            'chamber_abbrev': chamber_abbrev,
            'filenames': filenames,
        }
    if mode == 'dict':
        return data
    if mode == 'filenames':
        return [filename for item in data.values() for filename in item['filenames']]
    if mode == 'tuples':
        return [(item['chamber_abbrev'], filename) for item in data.values() for filename in item['filenames']]
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
            document_name: str = basename(path)
            protocol_year: str = document_name.split('-')[1]
            target_name: str = replace_extension(document_name, 'xml')
            target_sub_folder: str = target_folder if not create_subfolder else jj(target_folder, protocol_year)
            ensure_folder(target_sub_folder)
            shutil.copy(path, jj(target_sub_folder, target_name))


def create_tei_corpus_xml(source_folder: str, target_folder: str = None) -> None:
    """Creates a TEI Corpus XML file in the target folder."""

    target_folder = target_folder or source_folder

    def group_by_chambers(filenames: list[str]) -> dict[str, list[str]]:
        return {  # type: ignore
            chamber_abbrev: [(f.split('-')[1], f) for f in filenames if get_chamber_by_filename(f) == chamber_abbrev]
            for chamber_abbrev in ['ak', 'fk', 'ek']
        }

    template_dir: str = os.path.dirname(tp.__file__)

    try:
        template: Template = Environment(loader=FileSystemLoader(template_dir)).get_template("prot-xx.jinja")
    except TemplateNotFound as e:
        logger.error(f"template not found: {e}")
        return

    filenames: list[str] = strip_path_and_extension(ls_corpus_folder(source_folder, pattern='**/prot-*-*.xml'))
    chamber_protocols: dict[str, list[str]] = group_by_chambers(filenames)
    for chamber_id in chamber_protocols:
        filename: str = jj(target_folder, f"prot-{chamber_id}.xml")
        content: str = template.render(documents=chamber_protocols[chamber_id], chamber_id=chamber_id)
        with open(filename, mode="w", encoding="utf-8") as message:
            message.write(content)
            logger.info(f"... wrote {filename}")


def load_chamber_indexes(folder: str) -> dict[str, set[str]]:
    try:
        namespaces: dict[str, str] = {'xi': 'http://www.w3.org/2001/XInclude'}
        pattern: str = jj(folder, "prot-??.xml")
        chambers: dict[str, set[str]] = {}
        valid_chambers: set[str] = {'ak', 'fk', 'ek'}

        for filename in glob.glob(pattern):
            document_name: str = basename(filename)
            chamber_abbrev: str = document_name[5:7]
            if chamber_abbrev not in valid_chambers:
                logger.warning(f"illegal chamber: {chamber_abbrev} in file {filename}")

            root: ET.Element = ET.parse(filename).getroot()
            chambers[chamber_abbrev] = {
                basename(str(elem.get('href')))
                for elem in root.findall('.//xi:include', namespaces)
                if elem.get('href') is not None
            }

        return chambers
    except Exception as e:
        logger.warning(f"failed to load chamber indexes from {folder} {e}")
        return {}

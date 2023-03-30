import os
import re
from os.path import isfile
from os.path import join as jj
from posixpath import splitext

import pygit2
import requests
from loguru import logger

from ..utility import download_url_to_file, load_json, reset_folder
from .config import MetadataTableConfigs, input_unknown_url, table_url


def gh_ls(user: str, repository: str, path: str = "", tag: str = "main") -> list[dict]:
    url: str = f"https://api.github.com/repos/{user}/{repository}/contents/{path}?ref={tag}"
    data: list[dict] = load_json(url)
    return data


def gh_tags(folder: str = None) -> list[str]:
    """Returns tags in given local git repository"""
    repo: pygit2.Repository = pygit2.Repository(
        path=folder or jj(os.environ.get('RIKSPROT_DATA_FOLDER', '/data/riksdagen_corpus_data'), "riksdagen-corpus")
    )
    rx: re.Pattern = re.compile(r'^refs/tags/v\d+\.\d+\.\d+$')
    tags: list[str] = sorted([r.removeprefix('refs/tags/') for r in repo.references if rx.match(r)])
    return tags


def gh_dl_metadata(tag: str, folder: str = None, force: bool = False) -> dict:
    """Returns name, headers and content for each metadata CSV file for given tag. Optionally stores data in folder"""

    if folder is not None:
        reset_folder(folder, force=force)

    items: list[dict] = gh_ls("welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", tag)
    infos: dict[str, dict] = {}

    for item in items:
        table, extension = splitext(item.get("name"))
        if not extension.endswith("csv"):
            continue

        url: str = item.get("download_url", table_url(table, tag))
        data: str = requests.get(url, timeout=10).content.decode("utf-8")
        headers: list[str] = data.splitlines()[0].split(sep=',')

        infos[table] = {'name': table, 'headers': headers, 'content': data}

        if folder is not None:
            with open(jj(folder, f"{table}.csv"), 'w', encoding="utf-8") as fp:
                logger.info(f' -> downloaded {item.get("name", "")}')
                fp.write(data)

    return infos


def gh_dl_metadata_extra(folder: str, tag: str = "main", force: bool = False) -> list[str]:
    """Downloads version `tag`of riksprot metadata files to `folder`"""
    items: list[str] = gh_dl_metadata(tag=tag, folder=jj(folder, tag), force=force)
    download_url_to_file(input_unknown_url(tag=tag), jj(folder, tag, "unknowns.csv"))
    return items


def gh_dl_metadata_by_config(*, configs: MetadataTableConfigs, tag: str, folder: str, force: bool = False) -> None:
    """Downloads metadata files based on tables specified in `specifications`"""

    os.makedirs(folder, exist_ok=True)

    for tablename, config in configs.definitions.items():
        target_name: str = jj(folder, f"{tablename}.csv")
        if isfile(target_name):
            if not force:
                raise ValueError(f"File {target_name} exists, use `force=True` to overwrite")
            os.remove(target_name)
        logger.info(f"downloading {tablename} ({tag}) to {target_name}...")
        url: str = config.resolve_url(tag)
        download_url_to_file(url, target_name, force)

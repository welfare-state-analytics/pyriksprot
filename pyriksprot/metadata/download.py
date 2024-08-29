import os
from os.path import isfile
from os.path import join as jj
from posixpath import splitext
from typing import Any

import requests
from loguru import logger

from pyriksprot.gitchen import gh_get_url, gh_ls

from ..utility import dotget, download_url_to_file, reset_folder
from .schema import MetadataSchema


def gh_dl_metadata(
    *, target_folder: str, user: str, repository: str, tag: str, path: str = None, force: bool = False
) -> dict[str, dict]:
    """Returns name, headers and content for each metadata CSV file for given tag. Optionally stores data in folder"""

    if target_folder is not None:
        reset_folder(target_folder, force=force)

    items: list[dict] = gh_ls(user, repository, path, tag)
    infos: dict[str, dict] = {}

    for item in items:
        table, extension = splitext(item.get("name"))
        if not extension.endswith("csv"):
            continue

        url: str = gh_get_url(user=user, repository=repository, path=path, filename=f"{table}.csv", tag=tag)

        download_url_to_file(url, target_name, force)

        data: str = requests.get(url, timeout=10).content.decode("utf-8")
        headers: list[str] = data.splitlines()[0].split(sep=',')

        infos[table] = {'name': table, 'headers': headers, 'content': data}

        if target_folder is not None:
            with open(jj(target_folder, f"{table}.csv"), 'w', encoding="utf-8") as fp:
                logger.info(f' -> downloaded {item.get("name", "")}')
                fp.write(data)

    return infos


def _resolve_url(schema: MetadataSchema, tag: str, tablename: str) -> str:
    """Resolves proper URL to table for tag based on configuration"""
    if tablename not in schema.definitions:
        raise ValueError(f"Table {tablename} not found in configuration")
    url: Any = schema[tablename].url
    if url is None:
        return gh_get_url(
            user=dotget(schema.config, "github.user"),
            repository=dotget(schema.config, "github.repository"),
            path=dotget(schema.config, "github.path"),
            filename=f"{tablename}.csv",
            tag=tag,
        )

    if callable(url):
        return url(tag)

    return url


def gh_dl_metadata_by_config(*, schema: MetadataSchema, tag: str, folder: str, force: bool = False) -> None:
    """Downloads metadata files based on tables specified in `specifications`"""

    if force:
        reset_folder(folder, force=True)
    else:
        os.makedirs(folder, exist_ok=True)

    for tablename, _ in schema.definitions.items():

        if tablename.startswith(':'):
            continue
        
        target_name: str = jj(folder, f"{tablename}.csv")
        
        if isfile(target_name):

            if not force:
                raise ValueError(f"File {target_name} exists, use `force=True` to overwrite")
            
            os.remove(target_name)

        logger.info(f"downloading {tablename} ({tag}) to {target_name}...")

        url: str = _resolve_url(schema, tag, tablename)

        download_url_to_file(url, target_name, force, errors='raise')

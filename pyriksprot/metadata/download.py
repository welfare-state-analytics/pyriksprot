import os
from os.path import isfile
from os.path import join as jj
from posixpath import splitext
from typing import Any

import requests
from loguru import logger

from pyriksprot.gitchen import gh_create_url, gh_ls

from ..utility import dotget, download_url_to_file, fetch_text_by_url, reset_folder
from .schema import MetadataSchema


def gh_dl_metadata(
    *, target_folder: str, user: str, repository: str, tag: str, path: str = None, force: bool = False, errors='raise'
) -> dict[str, dict]:
    """Returns name, headers and content for each metadata CSV file for given tag. Optionally stores content in folder"""

    if target_folder is not None:
        reset_folder(target_folder, force=force)

    items: list[dict] = gh_ls(user, repository, path, tag)
    infos: dict[str, dict] = {}

    for item in items:
        table, extension = splitext(item.get("name"))
        if not extension.endswith("csv"):
            continue

        target_name: str = None if not target_folder else jj(target_folder, f"{table}.csv")

        url: str = gh_create_url(user=user, repository=repository, path=path, filename=f"{table}.csv", tag=tag)

        data: str = fetch_text_by_url(url, errors=errors)

        headers: list[str] = data.splitlines()[0].split(sep=',')

        infos[table] = {'name': table, 'headers': headers, 'content': data}

        if target_name is not None:
            with open(target_name, 'w', encoding="utf-8") as fp:
                logger.info(f' -> downloaded {item.get("name", "")}')
                fp.write(data)

    return infos


def _resolve_url(schema: MetadataSchema, tag: str, tablename: str) -> str:
    """Resolves proper URL to table for tag based on configuration"""
    if tablename not in schema.definitions:
        raise ValueError(f"Table {tablename} not found in configuration")
    url: Any = schema[tablename].url
    if url is None:
        return gh_create_url(
            user=dotget(schema.config, "github.user"),
            repository=dotget(schema.config, "github.repository"),
            path=dotget(schema.config, "github.path"),
            filename=f"{tablename}.csv",
            tag=tag,
        )

    if callable(url):
        return url(tag)

    return url


def gh_dl_metadata_by_config(*, schema: MetadataSchema, tag: str, folder: str, force: bool = False, errors='raise') -> None:
    """Downloads metadata files based on tables specified in `specifications`"""

    if force:
        reset_folder(folder, force=True)
    else:
        os.makedirs(folder, exist_ok=True)

    for tablename, _ in schema.definitions.items():

        if tablename.startswith(':'):
            continue
        
        target_name: str = jj(folder, f"{tablename}.csv")

        url: str = _resolve_url(schema, tag, tablename)

        download_url_to_file(url, target_name, force, errors=errors)

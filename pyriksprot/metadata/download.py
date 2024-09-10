import os
from os.path import basename
from os.path import join as jj
from typing import Any, Literal

from loguru import logger

from pyriksprot.gitchen import gh_create_url, gh_ls

from ..utility import dotget, download_url_to_file, fetch_text_by_url, reset_folder
from .schema import MetadataSchema, MetadataTable


def gh_fetch_metadata_item(
    table: str, tag: str, errors: Literal['raise', 'ignore'] = 'raise', **opts
) -> dict[str, Any]:
    """Fetches data from Github and returns name, headers and content for metadata CSV file for given tag"""
    url: str = (
        opts.get('url')
        if 'url' in opts
        else gh_create_url(
            user=opts.get('user'),
            repository=opts.get('repository'),
            path=opts.get('path'),
            filename=f"{table}.csv",
            tag=tag,
        )
    )

    data: str = fetch_text_by_url(url, errors=errors)

    headers: list[str] = data.splitlines()[0].split(sep=',')

    return {'name': table, 'headers': headers, 'content': data}


def gh_store_metadata_item(target_folder: str, item):
    if not target_folder:
        return
    target_name: str = jj(target_folder, item['name'])
    if target_name is not None:
        with open(target_name, 'w', encoding="utf-8") as fp:
            logger.info(f' -> downloaded {item.get("name", "")}')
            fp.write(item['content'])


def gh_fetch_metadata_folder(
    *,
    target_folder: str,
    user: str,
    repository: str,
    tag: str,
    path: str = None,
    force: bool = False,
    errors: Literal['raise', 'ignore'] = 'raise',
) -> dict[str, dict]:
    """Returns name, headers and content for each metadata CSV file for given tag. Optionally stores content in folder"""

    if target_folder is not None:
        reset_folder(target_folder, force=force)

    items: list[dict] = gh_ls(user, repository, path, tag, pattern="*.csv")
    infos: dict[str, dict] = {}

    for item in items:
        filename = basename(item["name"])

        infos[filename] = gh_fetch_metadata_item(filename, tag, errors, url=item.get("download_url"))

        gh_store_metadata_item(target_folder, infos[filename])

    return infos


def gh_fetch_metadata_by_config(
    *, schema: MetadataSchema, tag: str, folder: str, force: bool = False, errors: Literal['raise', 'ignore'] = 'raise'
) -> None:
    """Downloads metadata files based on tables specified in `specifications`"""

    if force:
        reset_folder(folder, force=True)
    else:
        os.makedirs(folder, exist_ok=True)

    gh_opts: dict[str, str] = dotget(schema.config, "github")

    for tablename, cfg in schema.definitions.items():
        if tablename.startswith(':'):
            continue

        if cfg.is_derived:
            continue

        url: str = _resolve_url(cfg, tag, **gh_opts)
        download_url_to_file(url, jj(folder, cfg.basename), force, errors=errors)


def _resolve_url(cfg: MetadataTable, tag: str, **opts) -> str:
    """Resolves proper URL to table for tag based on configuration"""
    url: Any = cfg.url
    if url is None:
        return gh_create_url(filename=cfg.basename, tag=tag, **opts)

    if callable(url):
        return url(tag)

    return url

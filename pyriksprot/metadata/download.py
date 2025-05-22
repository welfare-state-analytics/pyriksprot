import os
from os.path import join as jj
from typing import Any, Literal

from loguru import logger

from pyriksprot.gitchen import gh_create_url, gh_ls

from ..utility import dotget, download_url_to_file, fetch_text_by_url, reset_folder
from .schema import MetadataSchema, MetadataTable


def gh_download_file(filename: str, tag: str, errors: Literal['raise', 'ignore'] = 'raise', **opts) -> dict[str, Any]:
    """Fetches data from Github and returns name, headers and content for metadata CSV file for given tag"""
    url: str = (
        opts.get('url')
        if 'url' in opts
        else gh_create_url(
            user=opts.get('user'),
            repository=opts.get('repository'),
            path=opts.get('path'),
            filename=filename,
            tag=tag,
        )
    )

    data: str = fetch_text_by_url(url, errors=errors)

    headers: list[str] = data.splitlines()[0].split(sep=',')

    return {'name': filename, 'headers': headers, 'content': data}


def gh_store_file(target_folder: str, filename: str, content: str) -> None:
    if not target_folder:
        return
    target_name: str = jj(target_folder, filename)
    if target_name is not None:
        with open(target_name, 'w', encoding="utf-8") as fp:
            logger.info(f' -> downloaded {target_name}')
            fp.write(content)


def gh_download_files(
    target_folder: str, tag: str, errors: Literal['raise', 'ignore'], items: dict[str, str]
) -> dict[str, dict[str, str]]:
    infos: dict[str, dict] = {}

    for filename, url in items.items():
        infos[filename] = gh_download_file(filename, tag, errors, url=url)
        gh_store_file(target_folder, filename, infos[filename].get('content'))

    return infos


def gh_download_folder(
    *,
    target_folder: str,
    user: str,
    repository: str,
    tag: str,
    path: str = None,
    force: bool = False,
    errors: Literal['raise', 'ignore'] = 'raise',
    extras: dict[str, str] = None,
    **_,
) -> dict[str, dict[str, str]]:
    """Returns name, headers and content for each metadata CSV file for given tag. Optionally stores content in folder"""

    if target_folder is not None:
        reset_folder(target_folder, force=force)
        with open(jj(target_folder, 'version'), 'w', encoding="utf-8") as fp:
            fp.write(tag)

    items: dict[str, str] = {
        item.get("name"): item.get("download_url") for item in gh_ls(user, repository, path, tag, pattern="*.csv")
    } | (extras or {})

    infos: dict[str, dict[str, str]] = gh_download_files(target_folder, tag, errors, items)

    return infos


def gh_download_by_config(
    *, schema: MetadataSchema, version: str, folder: str, force: bool = False, errors: Literal['raise', 'ignore'] = 'raise'
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

        url: str = _resolve_url(cfg, version, **gh_opts)
        download_url_to_file(url, jj(folder, cfg.basename), force, errors=errors)


def _resolve_url(cfg: MetadataTable, tag: str, **opts) -> str:
    """Resolves proper URL to table for tag based on configuration"""
    url: Any = cfg.url
    if url is None:
        return gh_create_url(filename=cfg.basename, tag=tag, **opts)

    if callable(url):
        return str(url(tag))

    return url

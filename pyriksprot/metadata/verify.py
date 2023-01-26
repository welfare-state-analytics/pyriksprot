from __future__ import annotations

import glob
import os
from os.path import basename, splitext

from loguru import logger

from ..utility import repository_tags
from .config import RIKSPROT_METADATA_TABLES, table_url
from .generate import fx_or_url
from .utility import download_url

jj = os.path.join


def table_url2(tablename: str, tag: str = "main") -> str:
    specification: dict = RIKSPROT_METADATA_TABLES.get(tablename)
    return (
        table_url(tablename=tablename, tag=tag)
        if ':url:' not in specification
        else fx_or_url(specification[':url:'], tag)
    )


def verify_metadata_files(source_folder: str):

    ignore_filenames: list[str] = ["unknowns"]
    filenames: list[str] = [splitext(basename(x))[0] for x in glob.glob(f"{source_folder}/*.csv")]

    expected_filenames: list[str] = [x for x in RIKSPROT_METADATA_TABLES if x not in ignore_filenames]

    missing_filenames: list[str] = [x for x in expected_filenames if x not in filenames]
    unknown_filenames: list[str] = [x for x in filenames if x not in expected_filenames]

    if len(missing_filenames) > 0 or len(unknown_filenames) > 0:

        msg: str = ""

        if len(missing_filenames) > 0:
            msg += f"expected but not found: [{', '.join(missing_filenames)}]  "

        if len(unknown_filenames) > 0:
            msg += f"found but not expected: [{', '.join(unknown_filenames)}]"

        raise ValueError(msg)

    logger.info("metadata filenames is verified")


def verify_metadata_columns(tag1: str, tag2: str = None) -> None:

    tags: list[str] = repository_tags()
    errors: list[str] = []

    if tag1 not in tags:
        raise ValueError(f"unknown tag {tag1}")

    if tag2 is None:
        tag2 = tags[tags.index(tag1) - 1]

    tablenames: list[str] = [x for x in RIKSPROT_METADATA_TABLES]
    for tablename in tablenames:
        try:
            columns1, columns2 = [
                set(d.split('\n')[0].split(','))
                for d in [download_url(table_url2(tablename, tag)) for tag in (tag1, tag2)]
            ]

            if columns1.difference(columns2) or columns2.difference(columns1):

                if columns1.difference(columns2):
                    errors.append(f"{tablename}: unexpected column(s): {' '.join(columns1.difference(columns2))}")

                if columns2.difference(columns1):
                    errors.append(f"{tablename}: missing column(s): {' '.join(columns2.difference(columns1))}")

        except:  # pylint: disable=bare-except
            errors.append(f"{tablename}: comparision failed")

    if errors:
        raise ValueError("\n" + ("\n".join(errors)))

from __future__ import annotations

import os
import sqlite3

import pandas as pd
import requests


def revdict(d: dict) -> dict:
    return {v: k for k, v in d.items()}


def read_sql_table(table_name: str, con: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql(f"select * from {table_name}", con)


def read_sql_tables(tables: list[str] | dict, db: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    return tables if isinstance(tables, dict) else {table_name: read_sql_table(table_name, db) for table_name in tables}


def download_url_to_file(url: str, target_name: str, force: bool = False) -> None:

    if os.path.isfile(target_name):
        if not force:
            raise ValueError("File exists, use `force=True` to overwrite")
        os.unlink(target_name)

    ensure_path(target_name)

    with open(target_name, 'w', encoding="utf-8") as fp:
        data: str = requests.get(url, allow_redirects=True).content.decode("utf-8")
        fp.write(data)


def probe_filename(filename: list[str], exts: list[str] = None) -> str | None:
    """Probes existence of filename with any of given extensions in folder"""
    for probe_name in set([filename] + ([replace_extension(filename, ext) for ext in exts] if exts else [])):
        if os.path.isfile(probe_name):
            return probe_name
    raise FileNotFoundError(filename)


def replace_extension(filename: str, extension: str) -> str:
    if filename.endswith(extension):
        return filename
    base, _ = os.path.splitext(filename)
    return f"{base}{'' if extension.startswith('.') else '.'}{extension}"


def ensure_path(f: str) -> None:
    os.makedirs(os.path.dirname(f), exist_ok=True)

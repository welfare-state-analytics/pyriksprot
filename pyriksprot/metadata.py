from __future__ import annotations

import glob
import os
import shutil
import sqlite3
from contextlib import closing
from os.path import dirname, isfile, isdir
from typing import Any
from loguru import logger

import numpy as np
import pandas as pd
from tqdm import tqdm

from . import interface
from .parlaclarin import parse
from .utility import download_url_to_file

jj = os.path.join


def input_unknown_url(tag: str = "main"):
    return (
        f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/input/matching/unknowns.csv"
    )


RIKSPROT_METADATA_TABLES: dict = {
    'government': {
        # '+government_id': 'integer primary key',
        'government': 'text primary key not null',
        'start': 'date',
        'end': 'date',
        # ':options:': {'auto_increment': 'government_id'},
        ':index:': {},
    },
    'location_specifier': {
        # 'location_specifier_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'location': 'text',
    },
    'member_of_parliament': {
        # 'member_of_parliament_id': 'AUTO_INCREMENT',
        'person_id': 'text references person (person_id) not null',
        'party': 'text',
        'district': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
    },
    'minister': {
        'person_id': 'text references person (person_id) not null',
        'government': 'text',
        'role': 'text',
        'start': 'date',
        'end': 'date',
    },
    'name': {
        'person_id': 'text references person (person_id) not null',
        'name': 'text not null',
        'primary_name': 'integer not null',
    },
    'party_abbreviation': {
        'party': 'text',
        'abbreviation': 'text',
        'ocr_correction': 'text',
    },
    'party_affiliation': {
        'person_id': 'text references person (person_id) not null',
        'party': 'text',
    },
    'person': {
        'person_id': 'text primary key',
        'born': 'int',
        'dead': 'int',
        'gender': 'text',
        'wiki_id': 'text',
        'riksdagen_guid': 'text',
        'riksdagen_id': 'text',
    },
    'speaker': {
        'person_id': 'text references person (person_id) not null',
        'role': 'text',
        'start': 'date',
        'end': 'date',
    },
    'twitter': {
        'twitter': 'text',  # primary key',
        'person_id': 'text references person (person_id) not null',
    },
    'input_unknown': {
        'protocol_id': 'text',  # primary key',
        'hash': 'text',
        'gender': 'text',
        'party': 'text',
        'other': 'text',
        ':url:': input_unknown_url,
    },
}

METADATA_FILENAMES = [f"{x}.csv" for x in RIKSPROT_METADATA_TABLES.keys()]


def table_url(tablename: str, tag: str = "main") -> str:
    return f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/metadata/{tablename}.csv"


def register_numpy_adapters():
    for dt in [np.int8, np.int16, np.int32, np.int64]:
        sqlite3.register_adapter(dt, int)
    for dt in [np.float16, np.float32, np.float64]:
        sqlite3.register_adapter(dt, float)
    sqlite3.register_adapter(np.nan, lambda _: "'NaN'")
    sqlite3.register_adapter(np.inf, lambda _: "'Infinity'")
    sqlite3.register_adapter(-np.inf, lambda _: "'-Infinity'")


def smart_load_table(tablename: str, folder: str = None, branch: str = None, **opts) -> pd.DataFrame:
    if bool(folder is None) == bool(branch is None):
        raise ValueError("either folder or branch must be set - not both")
    if isinstance(branch, str):
        tablename: str = table_url(tablename, branch)
    elif isinstance(folder, str):
        tablename: str = jj(folder, f"{tablename}.csv")
    table: pd.DataFrame = pd.read_csv(tablename, **opts)
    return table


def download_to_folder(*, tag: str, folder: str, force: bool = False) -> None:

    if isdir(folder):
        if not force:
            raise ValueError("Folder exists, use `force=True` to overwrite")
        shutil.rmtree(folder, ignore_errors=True)

    os.makedirs(folder, exist_ok=True)

    for tablename in RIKSPROT_METADATA_TABLES:
        target_name: str = jj(folder, f"{tablename}.csv")
        url: str = table_url(tablename=tablename, tag=tag)
        download_url_to_file(url, target_name, force)
        # download_url(url, target_folder, filename)

    download_url_to_file(input_unknown_url(tag=tag), "input_unknown", force)

def fx_or_url(url: Any, tag: str) -> str:
    return url(tag) if callable(url) else url

def create_database(
    database_filename: str,
    branch: str = None,
    folder: str = None,
    force: bool = False,
):

    os.makedirs(dirname(database_filename), exist_ok=True)

    if isfile(database_filename):
        if not force:
            raise ValueError("DB exists, use `force=True` to overwrite")
        os.remove(database_filename)

    db = sqlite3.connect(database_filename)

    register_numpy_adapters()

    lf = '\n' + (12 * ' ')

    for tablename, specification in RIKSPROT_METADATA_TABLES.items():
        sql_ddl: str = f"""
            create table {tablename} (
                {(','+lf).join(f"{k.removeprefix('+')} {t}" for k, t in specification.items() if not k.startswith(":"))}
            );
        """
        # print(sql_ddl)
        with closing(db.cursor()) as cursor:
            cursor.executescript(sql_ddl)

    for tablename, specification in RIKSPROT_METADATA_TABLES.items():
        logger.info(f"loading table: {tablename}")

        column_names: list[str] = [k for k in specification if k[0] not in "+:"]

        if ':url:' in specification:
            table: pd.DataFrame = pd.read_csv(fx_or_url(specification[':url:'], branch), sep=',')
        else:
            table: pd.DataFrame = smart_load_table(tablename=tablename, folder=folder, branch=branch, sep=',')

        for c in table.columns:
            if table.dtypes[c] == np.dtype('bool'):
                table[c] = [int(x) for x in table[c]]

        data = table[column_names].to_records(index=False)

        with closing(db.cursor()) as cursor:
            insert_sql = f"""
            insert into {tablename} ({', '.join(column_names)})
                values ({', '.join(['?'] * len(column_names))});
            """
            # print(insert_sql, data[0])
            # print(table.columns)
            cursor.executemany(insert_sql, data)

    db.commit()

    return db


def generate_utterance_index(corpus_folder: str, target_folder: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:

    utterance_data: list[tuple] = []
    protocol_data: list[tuple[int, str]] = []
    filenames = glob.glob(jj(corpus_folder, "protocols", "**/*.xml"), recursive=True)

    for document_id, filename in tqdm(enumerate(filenames)):
        protocol: interface.Protocol = parse.ProtocolMapper.to_protocol(filename, segment_skip_size=1)
        protocol_data.append((document_id, protocol.name, protocol.date))
        for u in protocol.utterances:
            utterance_data.append(tuple([document_id, u.u_id, u.n, u.who]))

    data: tuple[pd.DataFrame, pd.DataFrame] = (
        pd.DataFrame(data=protocol_data, columns=['document_id', 'document_name', 'date']).set_index("document_id"),
        pd.DataFrame(data=utterance_data, columns=['document_id', 'u_id', 'hash', 'who']).set_index("u_id"),
    )

    if target_folder:

        for df, tablename in zip(data, ["protocols", "utterances"]):
            filename: str = jj(target_folder, f"{tablename}.csv")
            if os.path.isfile(filename):
                os.unlink(filename)
            df.to_csv(filename, sep="\t")

    return data


def load_utterance_index(database_filename: str, source_folder: str = None) -> None:

    folder: str = source_folder or dirname(database_filename)
    tablenames: list[str] = ["protocols", "utterances"]
    filenames: list[str] = [jj(folder, f"{x}.csv") for x in tablenames]

    if not all(isfile(x) for x in filenames):
        raise FileNotFoundError(f"files {' and/or '.join(filenames)} not found")

    if not isfile(database_filename):
        raise FileNotFoundError(database_filename)

    with closing(sqlite3.connect(database_filename)) as db:

        for tablename in tablenames:
            with closing(db.cursor()) as cursor:
                cursor.executescript(f"drop table if exists {tablename};")

        for tablename, filename in zip(tablenames, filenames):
            logger.info(f"loading table: {tablename}")
            data: pd.DataFrame = pd.read_csv(filename, sep='\t', index_col=0)
            data.to_sql(tablename, db, if_exists="replace")

        db.commit()


def load_scripts(database_filename: str, script_folder: str = None) -> None:

    script_folder: str = script_folder or jj(dirname(database_filename), "sql")

    if not isdir(script_folder):
        raise FileNotFoundError(script_folder)

    filenames = sorted(glob.glob(jj(script_folder, "*.sql")))

    with closing(sqlite3.connect(database_filename)) as db:

        for filename in filenames:
            logger.info(f"loading script: {os.path.split(filename)[1]}")
            with open(filename, "r", encoding="utf-8") as fp:
                sql_str: str = fp.read()
            with closing(db.cursor()) as cursor:
                cursor.executescript(sql_str)

        db.commit()

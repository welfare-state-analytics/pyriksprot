from __future__ import annotations

import glob
import os
import shutil
import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
import requests
from loguru import logger
from tqdm import tqdm

from . import interface
from .parlaclarin import parse
from .utility import download_url

jj = os.path.join

# create the dataframe from a query
# df = pd.read_sql_query("SELECT * FROM userdata", cnx)


RIKSPROT_METADATA_TABLES: dict = {
    'government': {
        '+government_id': 'integer primary key',
        'government': 'text not null',
        'start': 'date',
        'end': 'date',
        ':options:': {'auto_increment': 'government_id'},
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
}

RIKSPROT_METADATA_BASE_URL: str = (
    "https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{}/corpus/metadata/{}.csv"
)


def table_url(tablename: str, branch: str = "main") -> str:
    return RIKSPROT_METADATA_BASE_URL.format(branch, tablename)


def register_numpy_adapters():
    for dt in [np.int8, np.int16, np.int32, np.int64]:
        sqlite3.register_adapter(dt, int)
    for dt in [np.float16, np.float32, np.float64]:
        sqlite3.register_adapter(dt, float)
    sqlite3.register_adapter(np.nan, lambda _: "'NaN'")
    sqlite3.register_adapter(np.inf, lambda _: "'Infinity'")
    sqlite3.register_adapter(-np.inf, lambda _: "'-Infinity'")


def load_metadata_table(tablename: str, folder: str = None, branch: str = None, **opts) -> pd.DataFrame:
    if bool(folder is None) == bool(branch is None):
        raise ValueError("either folder or branch must be set - not both")
    if isinstance(branch, str):
        tablename: str = table_url(tablename, branch)
    elif isinstance(folder, str):
        tablename: str = os.path.join(folder, f"{tablename}.csv")
    table: pd.DataFrame = pd.read_csv(tablename, **opts)
    return table


def query_db(query: str, db: sqlite3.Connection) -> list[tuple]:
    with closing(db.cursor()) as cursor:
        return cursor.execute(query).fetchall()


def to_folder(branch: str, folder: str, force: bool = False) -> None:

    if os.path.isdir(folder):
        if not force:
            raise ValueError("Folder exists, use `force=True` to overwrite")
        shutil.rmtree(folder, ignore_errors=True)

    os.makedirs(folder, exist_ok=True)

    for tablename in RIKSPROT_METADATA_TABLES:
        target_name: str = os.path.join(folder, f"{tablename}.csv")
        with open(target_name, 'w', encoding="utf-8") as fp:
            url: str = table_url(tablename=tablename, branch=branch)
            data: str = requests.get(url, allow_redirects=True).content.decode("utf-8")
            fp.write(data)


def create_metadata_db(database_name: str, branch: str = None, folder: str = None, force: bool = False):

    if os.path.isfile(database_name):
        if not force:
            raise ValueError("DB exists, use `force=True` to overwrite")
        os.remove(database_name)

    db = sqlite3.connect(database_name)

    register_numpy_adapters()

    sqlite3.register_adapter(np.int64, int)

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

        column_names: list[str] = [k for k in specification if k[0] not in "+:"]

        table: pd.DataFrame = load_metadata_table(tablename=tablename, folder=folder, branch=branch, sep=',')

        for c in table.columns:
            if table.dtypes[c] == np.dtype('bool'):
                table[c] = [int(x) for x in table[c]]

        data = table[column_names].to_records(index=False)

        with closing(db.cursor()) as cursor:
            insert_sql = f"""
            insert into {tablename} ({', '.join(column_names)})
                values ({', '.join(['?'] * len(column_names))});
            """
            print(tablename)
            # print(insert_sql, data[0])
            # print(table.columns)
            cursor.executemany(insert_sql, data)

    db.commit()
    return db


METADATA_FILENAMES = [f"{x}.csv" for x in RIKSPROT_METADATA_TABLES.keys()]


def _metadata_url(*, filename: str, tag: str) -> str:
    return (
        f'https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/metadata/{filename}'
    )


def download_metadata(target_folder: str, tag: str = "main"):

    shutil.rmtree(target_folder, ignore_errors=True)
    os.makedirs(target_folder, exist_ok=True)
    logger.info("downloading parliamentary metadata")

    for filename in METADATA_FILENAMES:
        url: str = _metadata_url(filename=filename, tag=tag)
        download_url(url=url, target_folder=target_folder, filename=filename)


def collect_utterance_whos(corpus_folder: str) -> pd.DataFrame:
    data: list[tuple] = []
    filenames = glob.glob(jj(corpus_folder, "protocols", "**/*.xml"), recursive=True)
    for filename in tqdm(filenames):
        protocol: interface.Protocol = parse.ProtocolMapper.to_protocol(filename, segment_skip_size=1)
        for u in protocol.utterances:
            data.append(tuple([protocol.name, protocol.date, u.n, u.who]))
    df = pd.DataFrame(data=data, columns=['protocol_name', 'date', 'hash', 'who'])
    return df

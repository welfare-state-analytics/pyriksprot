from __future__ import annotations

import abc
import glob
import os
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from os.path import dirname, isdir, isfile
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

from .utility import download_url_to_file, probe_filename

jj = os.path.join


class IParser(abc.ABC):

    """IParser = parse.ProtocolMapper """

    @dataclass
    class IUtterance:
        u_id: str
        who: str
        speaker_note_id: str

    @dataclass
    class IProtocol:
        name: str
        date: str
        utterances: list[IParser.IUtterance]

        def get_speaker_notes(self) -> dict[str, str]:
            return {}

    def to_protocol(
        self, filename: str, segment_skip_size: int, ignore_tags: set[str]  # pylint: disable=unused-argument
    ) -> IParser.IProtocol:
        ...


# pylint: disable=unsupported-assignment-operation, unsubscriptable-object


def input_unknown_url(tag: str = "main"):
    return (
        f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/input/matching/unknowns.csv"
    )


def table_url(tablename: str, tag: str = "main") -> str:
    return f"https://raw.githubusercontent.com/welfare-state-analytics/riksdagen-corpus/{tag}/corpus/metadata/{tablename}.csv"


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
        'start': 'int',
        'end': 'int',
        # ':loaded_hook:': _to_year_period
    },
    'person': {
        'person_id': 'text primary key',
        'born': 'int',
        'dead': 'int',
        'gender': 'text',
        'wiki_id': 'text',
        'riksdagen_guid': 'text',
        'riksdagen_id': 'text',
        ':drop_duplicates:': 'person_id',
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
    'unknowns': {
        'protocol_id': 'text',  # primary key',
        'uuid': 'text',
        'gender': 'text',
        'party': 'text',
        'other': 'text',
        ':url:': input_unknown_url,
    },
}

EXTRA_TABLES = {
    'speech_index': {
        'document_id': 'int primary key',
        'document_name': 'text',
        'year': 'int',
        'who': 'text',
        'gender_id': 'int',
        'party_id': 'int',
        'office_type_id': 'int',
        'sub_office_type_id': 'int',
        'n_tokens': 'int',
        'filename': 'text',
        'u_id': 'text',
        'n_utterances': 'int',
        'speaker_note_id': 'text',
        'speach_index': 'int',
    },
}


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
    if isinstance(folder, str):
        tablename: str = probe_filename(jj(folder, f"{tablename}.csv"), ["zip", "csv.gz"])
    elif isinstance(branch, str):
        tablename: str = table_url(tablename, branch)
    table: pd.DataFrame = pd.read_csv(tablename, **opts)
    return table


def download_to_folder(*, tag: str, folder: str, force: bool = False) -> None:

    os.makedirs(folder, exist_ok=True)

    for tablename, specification in RIKSPROT_METADATA_TABLES.items():
        target_name: str = jj(folder, f"{tablename}.csv")
        if isfile(target_name):
            if not force:
                raise ValueError(f"File {target_name} exists, use `force=True` to overwrite")
            os.remove(target_name)
        logger.info(f"downloading {tablename} ({tag}) to {target_name}...")
        url: str = (
            table_url(tablename=tablename, tag=tag)
            if ':url:' not in specification
            else fx_or_url(specification[':url:'], tag)
        )
        download_url_to_file(url, target_name, force)
        # download_url(url, target_folder, filename)


def subset_to_folder(parser: IParser, source_folder: str, source_metadata: str, target_folder: str):
    """Creates a subset of metadata in source metadata that includes only protocols found in source_folder"""

    logger.info("Subsetting metadata database.")
    logger.info(f"    Source folder: {source_folder}")
    logger.info(f"  Source metadata: {source_metadata}")
    logger.info(f"    Target folder: {target_folder}")

    data: tuple = generate_corpus_indexes(parser, corpus_folder=source_folder, target_folder=target_folder)

    protocols: pd.DataFrame = data[0]
    utterances: pd.DataFrame = data[1]
    # speaker_notes: pd.DataFrame = data[2]

    person_ids: list[str] = set(utterances.person_id.unique().tolist())
    logger.info(f"found {len(person_ids)} unqiue persons in subsetted utterances.")

    for tablename in ["government", "party_abbreviation"]:
        shutil.copy(jj(source_metadata, f"{tablename}.csv"), jj(target_folder, f"{tablename}.csv"))

    person_tables: list[str] = [
        "location_specifier",
        "member_of_parliament",
        "minister",
        "name",
        "party_affiliation",
        "person",
        "speaker",
        "twitter",
    ]

    for tablename in person_tables:
        filename: str = f"{tablename}.csv"
        table: pd.DataFrame = pd.read_csv(jj(source_metadata, filename), sep=',', index_col=None)
        table = table[table['person_id'].isin(person_ids)]
        table.to_csv(jj(target_folder, filename), sep=',', index=False)

    unknowns: pd.DataFrame = pd.read_csv(jj(source_metadata, "unknowns.csv"), sep=',', index_col=None)
    unknowns = unknowns[unknowns['protocol_id'].isin({f"{x}.xml" for x in protocols['document_name']})]
    unknowns.to_csv(jj(target_folder, "unknowns.csv"), sep=',', index=False)


def fx_or_url(url: Any, tag: str) -> str:
    return url(tag) if callable(url) else url


def sql_ddl_create(*, tablename: str, specification: dict[str, str]) -> str:
    lf = '\n' + (12 * ' ')
    sql_ddl: str = f"""
        create table {tablename} (
            {(','+lf).join(f"{k.removeprefix('+')} {t}" for k, t in specification.items() if not k.startswith(":"))}
        );
    """
    return sql_ddl


def sql_ddl_insert(*, tablename: str, columns: list[str]) -> str:
    insert_sql = f"""
    insert into {tablename} ({', '.join(columns)})
        values ({', '.join(['?'] * len(columns))});
    """
    return insert_sql


def create_database(
    database_filename: str,
    branch: str = None,
    folder: str = None,
    force: bool = False,
):
    logger.info(f"Creating database {database_filename}, using source {branch}/{folder} (tag/folder).")

    os.makedirs(dirname(database_filename), exist_ok=True)

    if isfile(database_filename):
        if not force:
            raise ValueError("DB exists, use `force=True` to overwrite")
        os.remove(database_filename)

    db = sqlite3.connect(database_filename)

    register_numpy_adapters()

    for tablename, specification in RIKSPROT_METADATA_TABLES.items():
        with closing(db.cursor()) as cursor:
            cursor.executescript(sql_ddl_create(tablename=tablename, specification=specification))

    for tablename, specification in RIKSPROT_METADATA_TABLES.items():
        logger.info(f"loading table: {tablename}")

        if ':url:' in specification and folder is None:
            table: pd.DataFrame = pd.read_csv(fx_or_url(specification[':url:'], branch), sep=',')
        else:
            table: pd.DataFrame = smart_load_table(tablename=tablename, folder=folder, branch=branch, sep=',')

        if ':drop_duplicates:' in specification:
            table = table.drop_duplicates(subset='person_id', keep='first')

        # if ':loaded_hook:' in specification:
        #     table = specification[':loaded_hook:'](table)

        for c in table.columns:
            if table.dtypes[c] == np.dtype('bool'):  # pylint: disable=no-member
                table[c] = [int(x) for x in table[c]]

        with closing(db.cursor()) as cursor:
            columns: list[str] = [k for k in specification if k[0] not in "+:"]
            data = table[columns].to_records(index=False)
            insert_sql = sql_ddl_insert(tablename=tablename, columns=columns)
            cursor.executemany(insert_sql, data)

    db.commit()
    return db


def generate_corpus_indexes(
    parser: IParser, corpus_folder: str, target_folder: str = None
) -> tuple[pd.DataFrame, pd.DataFrame]:

    logger.info("Generating utterance, protocol and speaker notes indices.")
    logger.info(f"  source: {corpus_folder}")
    logger.info(f"  target: {target_folder}")

    utterance_data: list[tuple] = []
    protocol_data: list[tuple[int, str]] = []
    filenames = glob.glob(jj(corpus_folder, "protocols", "**/*.xml"), recursive=True)
    speaker_notes: dict[str, str] = {}

    for document_id, filename in tqdm(enumerate(filenames)):
        protocol: IParser.IProtocol = parser.to_protocol(filename, segment_skip_size=0, ignore_tags={"teiHeader"})
        protocol_data.append((document_id, protocol.name, protocol.date, int(protocol.date[:4])))
        for u in protocol.utterances:
            utterance_data.append(tuple([document_id, u.u_id, u.who, u.speaker_note_id]))
        speaker_notes.update(protocol.get_speaker_notes())

    data: tuple[pd.DataFrame, pd.DataFrame] = (
        pd.DataFrame(data=protocol_data, columns=['document_id', 'document_name', 'date', 'year']).set_index(
            "document_id"
        ),
        pd.DataFrame(data=utterance_data, columns=['document_id', 'u_id', 'person_id', 'speaker_note_id']).set_index(
            "u_id"
        ),
        pd.DataFrame(speaker_notes.items(), columns=['speaker_note_id', 'speaker_note']).set_index('speaker_note_id'),
    )

    if target_folder:

        os.makedirs(target_folder, exist_ok=True)

        for df, tablename in zip(data, ["protocols", "utterances", "speaker_notes"]):
            filename: str = jj(target_folder, f"{tablename}.csv")
            if os.path.isfile(filename):
                os.unlink(filename)
            df.to_csv(filename, sep="\t")

    return data


def load_corpus_indexes(*, database_filename: str, source_folder: str = None) -> None:

    folder: str = source_folder or dirname(database_filename)
    tablenames: list[str] = ["protocols", "utterances", "speaker_notes"]
    filenames: list[str] = [probe_filename(jj(folder, f"{x}.csv"), ["zip", "csv.gz"]) for x in tablenames]

    if not isfile(database_filename):
        raise FileNotFoundError(database_filename)

    if not all(isfile(filename) for filename in filenames):
        raise FileNotFoundError(','.join(filenames))

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

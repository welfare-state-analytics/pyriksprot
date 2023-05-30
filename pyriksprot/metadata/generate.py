from __future__ import annotations

import os
import sqlite3
from glob import glob
from os.path import isdir, isfile

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot.interface import IProtocol, IProtocolParser, SpeakerNote

from ..sql import sql_file_paths
from ..utility import ensure_path, probe_filename, reset_file
from . import config as cfg
from . import utility

jj = os.path.join


# pylint: disable=unsupported-assignment-operation, unsubscriptable-object


class DatabaseHelper:
    def __init__(self, filename: str):
        self.filename: str = filename if isinstance(filename, str) else None
        self.connection: sqlite3.Connection = None

        utility.register_numpy_adapters()

    def open(self):
        self.connection = sqlite3.connect(self.filename)

    def close(self):
        if self.connection is None:
            return
        self.connection.commit()
        self.connection.close()
        self.connection = None

    def commit(self) -> None:
        if self.connection is None:
            return
        self.connection.commit()

    def __enter__(self):
        self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_tag(self) -> str | None:
        with self:
            return (self.connection.execute("select version from version").fetchone() or [None])[0]

    def set_tag(self, tag: str) -> None:
        sql: str = f"""
            create table if not exists version (
                version text
            );
            delete from version;
            insert into version(version) values ('{tag}');
        """
        with self:
            self.connection.executescript(sql).close()

    def verify_tag(self, tag: str) -> DatabaseHelper:
        if self.get_tag() != tag:
            raise ValueError(f"metadata version mismatch: db version {self.get_tag()} differs from {tag}")
        return self

    def reset(self, tag: str, force: bool) -> DatabaseHelper:
        ensure_path(self.filename)
        reset_file(self.filename, force=force)

        if tag is None:
            raise ValueError("Git version tag cannot be NULL!")

        self.set_tag(tag)
        return self

    def create(self, tag: str = None, folder: str = None, force: bool = False):
        logger.info(f"Creating database {self.filename}, using source {tag}/{folder} (tag/folder).")

        configs: cfg.MetadataTableConfigs = cfg.MetadataTableConfigs()

        self.reset(tag=tag, force=force)
        self.create_base_tables(configs)
        self.load_base_tables(configs, folder)

        return self

    def create_base_tables(self, configs: cfg.MetadataTableConfigs) -> DatabaseHelper:
        with self:
            for _, config in configs.items():
                self.connection.executescript(config.to_sql_create()).close()
        return self

    def load_base_tables(self, configs: cfg.MetadataTableConfigs, folder: str) -> DatabaseHelper:
        tag: str = self.get_tag()

        with self:
            for _, config in configs.items():
                self.load_base_table(config, folder, tag)

        return self

    def load_base_table(self, config: cfg.MetadataTableConfig, folder: str, tag: str) -> DatabaseHelper:
        logger.info(f"loading table: {config.name}")
        table: pd.DataFrame = config.load_table(folder, tag)
        transformed_table: pd.DataFrame = config.transform(table)[config.all_columns]
        logger.warning(f"{','.join(transformed_table.columns)}")
        data: np.recarray = transformed_table.to_records(index=False)
        self.connection.executemany(config.to_sql_insert(), data).close()
        return self

    def load_scripts(self, folder: str = None) -> DatabaseHelper:
        """Loads SQL files from specified folder otherwise loads files in sql module"""

        if folder and not isdir(folder):
            raise FileNotFoundError(folder)

        filenames: list[str] = sorted(glob(jj(folder, "*.sql"))) if folder else sql_file_paths()

        with self:
            for filename in filenames:
                logger.info(f"loading script: {os.path.split(filename)[1]}")
                with open(filename, "r", encoding="utf-8") as fp:
                    sql_str: str = fp.read()
                self.connection.executescript(sql_str).close()

        return self

    def load_corpus_indexes(self, *, folder: str) -> DatabaseHelper:
        """Loads corpus indexes into iven database."""

        tablenames: list[str] = ["protocols", "utterances", "speaker_notes"]
        filenames: list[str] = [probe_filename(jj(folder, f"{x}.csv"), ["zip", "csv.gz"]) for x in tablenames]

        if not all(isfile(filename) for filename in filenames):
            raise FileNotFoundError(','.join(filenames))

        with self:
            for tablename in tablenames:
                self.connection.executescript(f"drop table if exists {tablename};").close()

            for tablename, filename in zip(tablenames, filenames):
                logger.info(f"loading table: {tablename}")
                pd.read_csv(filename, sep='\t', index_col=0).to_sql(tablename, self.connection, if_exists="replace")

        return self

    def load_data_tables(self, data_tables: dict[str, str | None]):
        with self:
            data: dict[str, pd.DataFrame] = utility.load_tables(data_tables, db=self.connection)
            return data


class CorpusIndexFactory:
    def __init__(self, parser: IProtocolParser) -> None:
        self.parser = parser
        self.data: dict[str, pd.DataFrame]

    def generate(self, corpus_folder: str, target_folder: str) -> CorpusIndexFactory:
        if os.path.isdir(jj(corpus_folder, "protocols")):
            corpus_folder = jj(corpus_folder, "protocols")
        logger.info("Corpus index: generating utterance, protocol, speaker notes and page reference indices.")
        logger.info(f"     Source: {corpus_folder}")
        logger.info(f"     Target: {target_folder}")

        filenames = glob(jj(corpus_folder, "**/*.xml"), recursive=True)

        return self.collect(filenames).store(target_folder)

    def _empty(self) -> pd.DataFrame:
        """Find and store protocols without any utterance."""
        protocols: pd.DataFrame = self.data["protocols"]
        protocol_ids: set[int] = set(self.data["utterances"].document_id.unique())
        return protocols[~protocols.index.isin(protocol_ids)]

    def collect(self, filenames) -> CorpusIndexFactory:
        utterance_data: list[tuple] = []
        page_reference_data: list[tuple] = []
        protocol_data: list[tuple[int, str]] = []
        speaker_notes: dict[str, SpeakerNote] = {}

        for document_id, filename in tqdm(enumerate(filenames)):
            protocol: IProtocol = self.parser.parse(filename, ignore_tags={"teiHeader"})
            protocol_data.append((document_id, protocol.name, protocol.date, int(protocol.date[:4])))
            utterance_data.extend(
                tuple([document_id, u.u_id, u.who, u.speaker_note_id, u.page_number]) for u in protocol.utterances
            )
            page_reference_data.extend(
                tuple([document_id, p.source_id, p.page_number, p.reference]) for p in protocol.page_references
            )

            speaker_notes.update(protocol.get_speaker_notes())

        """Store enough source reference data to reconstruct source urls."""
        page_references: pd.DataFrame = pd.DataFrame(
            data=page_reference_data, columns=['document_id', 'source_id', 'page_number', 'reference']
        ).set_index(['document_id', 'page_number'], drop=True)

        source_references: pd.DataFrame = (
            page_references[page_references.reference != ""]
            .reset_index()[['document_id', 'source_id', 'reference']]
            .copy()
            .drop_duplicates()
        ).set_index(['document_id'], drop=True)
        page_references.drop(columns=['reference'], inplace=True)

        speaker_notes: pd.DataFrame = pd.DataFrame(
            ((x.speaker_note_id, x.speaker_note) for x in speaker_notes.values()),
            columns=['speaker_note_id', 'speaker_note'],
        ).set_index('speaker_note_id')

        self.data = {
            "protocols": pd.DataFrame(
                data=protocol_data, columns=['document_id', 'document_name', 'date', 'year']
            ).set_index("document_id"),
            "utterances": pd.DataFrame(
                data=utterance_data,
                columns=['document_id', 'u_id', 'person_id', 'speaker_note_id', 'page_number'],
            ).set_index("u_id"),
            "page_references": page_references,
            'source_references': source_references,
            "speaker_notes": speaker_notes,
        }
        self.data['empty_protocols'] = self._empty()

        return self

    def store(self, target_folder: str) -> CorpusIndexFactory:
        if target_folder:
            os.makedirs(target_folder, exist_ok=True)

            for tablename, df in self.data.items():
                filename: str = jj(target_folder, f"{tablename}.csv.gz")
                df.to_csv(filename, sep="\t")

            logger.info("Corpus index: stored.")

        return self

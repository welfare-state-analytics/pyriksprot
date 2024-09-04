from __future__ import annotations

import os
from glob import glob
from os.path import isdir, isfile, join
from typing import Self

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot.gitchen import gh_create_url
from pyriksprot.interface import IProtocol, IProtocolParser, SpeakerNote
from pyriksprot.metadata import database

from ..sql import sql_file_paths
from ..utility import probe_filename
from .schema import MetadataSchema, MetadataTable

jj = os.path.join


# pylint: disable=unsupported-assignment-operation, unsubscriptable-object

INDEX_TABLE_NAMES: list[str] = ["protocols", "utterances", "speaker_notes"]


class GenerateService:
    def __init__(self, **opts) -> None:
        self.opts: dict[str, str] = opts
        self.db: database.DatabaseInterface = (
            opts['db']
            if isinstance(opts.get('db'), database.DatabaseInterface)
            else database.DefaultDatabaseType(**opts)
        )

    def verify_tag(self, tag: str) -> GenerateService:
        if self.db.version != tag:
            raise ValueError(f"metadata version mismatch: db version {self.db.version} differs from {tag}")
        return self

    def create(self, tag: str = None, folder: str = None, scripts_folder: str = None, force: bool = False) -> Self:
        logger.info(f"Creating database for tag '{tag}' using folder '{folder}'.")

        schema: MetadataSchema = MetadataSchema(tag)

        self.db.create_database(tag=tag, force=force)

        with self.db:
            self._create_tables(schema)
            self.upload_metadata(schema, folder)

            if scripts_folder:
                self.execute_sql_scripts(folder=scripts_folder, tag=tag)

        return self

    def _create_tables(self, schema: MetadataSchema) -> GenerateService:
        for tablename, cfg in schema.items():
            self.db.create(tablename, cfg.all_columns_specs, cfg.constraints)
        return self

    def upload_metadata(self, schema: MetadataSchema, folder: str) -> GenerateService:
        for _, cfg in schema.items():
            self._import_table(cfg, folder=folder, tag=self.db.version)

        return self

    def _import_table(self, cfg: MetadataTable, folder: str, tag: str) -> GenerateService:
        logger.info(f"loading table: {cfg.tablename}")

        columns: list[str] = cfg.all_columns
        table: pd.DataFrame = load(cfg.basename, url=cfg.url, folder=folder, tag=tag)
        self.db.store(cfg.transform(table)[columns], tablename=cfg.tablename, columns=columns)

        return self

    def execute_sql_scripts(self, *, folder: str = None, tag: str = None) -> GenerateService:
        """Loads SQL files from specified folder otherwise loads files in sql module"""

        with self.db:
            if not (folder or tag):
                raise ValueError("Either folder or tag must be specified.")

            if folder and not isdir(folder):
                raise FileNotFoundError(folder)

            filenames: list[str] = sorted(glob(jj(folder, "*.sql"))) if folder else sql_file_paths(tag=tag)

            with self.db:
                for filename in filenames:
                    self.db.load_script(filename=filename)

        return self

    def upload_corpus_indexes(self, *, folder: str, index_tables: list[str] = None) -> GenerateService:
        """Loads corpus indexes into given database."""

        with self.db:
            tablenames: list[str] = index_tables or INDEX_TABLE_NAMES
            filenames: list[str] = [probe_filename(jj(folder, f"{x}.csv"), ["zip", "csv.gz"]) for x in tablenames]

            if not all(isfile(filename) for filename in filenames):
                raise FileNotFoundError(','.join(filenames))

            for tablename in tablenames:
                self.db.drop(tablename)

            for tablename, filename in zip(tablenames, filenames):
                data: pd.DataFrame = pd.read_csv(filename, sep='\t', index_col=0)
                self.db.store2(data=data, tablename=tablename)

        return self

    def load_data_tables(self, data_tables: dict[str, str | None]):
        with self.db:
            data: dict[str, pd.DataFrame] = self.db.fetch_tables(data_tables)
            return data


def load(tablename: str, sep: str = ',', **opts) -> pd.DataFrame:
    """Loads table from specified folder or from url in configuration"""

    if opts.get("url"):
        return pd.read_csv(opts['url'], sep=sep)

    if opts.get("folder"):
        folder: str = probe_filename(join(opts['folder'], tablename), ['csv', "zip", "csv.gz"])
        return pd.read_csv(folder, sep=sep)

    if opts.get("tag"):
        if not all(opts.get(x) for x in ["user", "repository", "path"]):
            raise ValueError("when fetching from Github user, repository and path must be set")

        url: str = gh_create_url(
            filename=tablename,
            tag=opts.get("tag"),
            user=opts.get("user"),
            repository=opts.get("repository"),
            path=opts.get("path"),
        )
        return pd.read_csv(url)

    raise ValueError("either :url:, folder or branch must be set")


class CorpusIndexFactory:
    def __init__(self, parser: IProtocolParser) -> None:
        self.parser = parser
        self.data: dict[str, pd.DataFrame]

    def generate(self, corpus_folder: str, target_folder: str) -> CorpusIndexFactory:
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

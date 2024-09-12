from __future__ import annotations

import os
from typing import Type

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus.utility import ls_corpus_folder
from pyriksprot.interface import IProtocol, IProtocolParser, SpeakerNote

from . import database
from .schema import MetadataSchema

jj = os.path.join


class CorpusIndexFactory:
    def __init__(self, parser: IProtocolParser | Type[IProtocolParser], schema: MetadataSchema | str) -> None:
        self.parser: IProtocolParser | Type[IProtocolParser] = parser
        self.data: dict[str, pd.DataFrame] = {}
        self.sep: str = '\t'
        self.schema: MetadataSchema = schema if isinstance(schema, MetadataSchema) else MetadataSchema(schema)

    def generate(self, corpus_folder: str, target_folder: str) -> CorpusIndexFactory:
        logger.info("Corpus index: generating utterance, protocol, speaker notes and page reference indices.")
        logger.info(f"     Source: {corpus_folder}")
        logger.info(f"     Target: {target_folder}")
        logger.info(f"     Pattern: {target_folder}")

        filenames: list[str] = ls_corpus_folder(corpus_folder)

        return self.collect(filenames).to_csv(target_folder)

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
            page_references[page_references.reference != ""]  # pylint: disable=unsubscriptable-object
            .reset_index()[['document_id', 'source_id', 'reference']]
            .copy()
            .drop_duplicates()
        ).set_index(['document_id'], drop=True)
        page_references.drop(columns=['reference'], inplace=True)

        speaker_notes_data: pd.DataFrame = pd.DataFrame(
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
            "speaker_notes": speaker_notes_data,
        }
        self.data['empty_protocols'] = self._empty()

        return self

    def to_csv(self, folder: str) -> CorpusIndexFactory:
        if folder:
            os.makedirs(folder, exist_ok=True)

            for tablename, df in self.data.items():
                filename: str = jj(folder, f"{tablename}.csv.gz")
                df.to_csv(filename, sep=self.sep)

            logger.info("Corpus index: stored.")

        return self

    def upload(self, *, db: database.DatabaseInterface, folder: str) -> CorpusIndexFactory:
        """Loads corpus indexes into given database."""

        with db:
            db.set_deferred(True)
            for cfg in self.schema.derived_tables:
                data: pd.DataFrame = (
                    self.data[cfg.tablename].reset_index()
                    if self.data
                    else pd.read_csv(jj(folder, cfg.basename), sep=cfg.sep, index_col=None)
                )
                db.store(data=data, tablename=cfg.tablename)

        return self

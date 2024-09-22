from __future__ import annotations

import itertools
import os
from dataclasses import dataclass, field
from typing import Sequence, Type

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus.utility import ls_corpus_folder
from pyriksprot.interface import MISSING_SPEAKER_NOTE, IProtocol, IProtocolParser, SpeakerNote

from .schema import MetadataSchema

jj = os.path.join


class CorpusScanner:

    @dataclass
    class ScanResult:
        protocols: list[tuple[int, str]] = field(default_factory=list)
        utterances: list[tuple] = field(default_factory=list)
        page_references: list[tuple] = field(default_factory=list)
        speaker_notes: dict[str, SpeakerNote] = field(default_factory=dict)

    def __init__(self, parser: IProtocolParser | Type[IProtocolParser]) -> None:
        self.parser: IProtocolParser | Type[IProtocolParser] = parser

    def scan(self, filenames: Sequence[str]) -> CorpusScanner.ScanResult:

        data: CorpusScanner.ScanResult = CorpusScanner.ScanResult()

        for document_id, filename in tqdm(enumerate(filenames)):
            protocol: IProtocol = self.parser.parse(filename, ignore_tags={"teiHeader"})
            data.protocols.append((document_id, protocol.name, protocol.date, int(protocol.date[:4])))
            data.utterances.extend(
                tuple([document_id, u.u_id, u.who, u.speaker_note_id, u.page_number]) for u in protocol.utterances
            )
            data.page_references.extend(
                tuple([document_id, p.source_id, p.page_number, p.reference]) for p in protocol.page_references
            )

            data.speaker_notes.update(protocol.get_speaker_notes())

        return data

    # def parallel_scan(self, filenames: Sequence[str]) -> CorpusScanner.ScanResult:
    #     data: CorpusScanner.ScanResult = CorpusScanner.ScanResult()

    #     # Function to process a single file
    #     def process_file(document_id, filename):
    #         protocol: IProtocol = self.parser.parse(filename, ignore_tags={"teiHeader"})
    #         protocols = (document_id, protocol.name, protocol.date, int(protocol.date[:4]))
    #         utterances = [
    #             (document_id, u.u_id, u.who, u.speaker_note_id, u.page_number) for u in protocol.utterances
    #         ]
    #         page_references = [
    #             (document_id, p.source_id, p.page_number, p.reference) for p in protocol.page_references
    #         ]
    #         speaker_notes = protocol.get_speaker_notes()
    #         return protocols, utterances, page_references, speaker_notes

    #     # Parallel processing with ThreadPoolExecutor
    #     with ThreadPoolExecutor() as executor:
    #         futures = {
    #             executor.submit(process_file, document_id, filename): filename
    #             for document_id, filename in enumerate(filenames)
    #         }

    #         # Process each future as it completes
    #         for future in tqdm(as_completed(futures), total=len(futures)):
    #             protocols, utterances, page_references, speaker_notes = future.result()

    #             # Append and extend data to the main ScanResult object
    #             data.protocols.append(protocols)
    #             data.utterances.extend(utterances)
    #             data.page_references.extend(page_references)
    #             data.speaker_notes.update(speaker_notes)

    #     return data

    def to_dataframes(self, result: CorpusScanner.ScanResult) -> dict[str, pd.DataFrame]:
        """Store enough source reference data to reconstruct source urls."""

        protocols: pd.DataFrame = pd.DataFrame(
            data=result.protocols, columns=['document_id', 'document_name', 'date', 'year']
        ).set_index("document_id")

        utterances: pd.DataFrame = pd.DataFrame(
            data=result.utterances,
            columns=['document_id', 'u_id', 'person_id', 'speaker_note_id', 'page_number'],
        ).set_index("u_id")

        page_references: pd.DataFrame = pd.DataFrame(
            data=result.page_references, columns=['document_id', 'source_id', 'page_number', 'reference']
        ).set_index(['document_id', 'page_number'], drop=True)

        source_references: pd.DataFrame = (
            page_references[page_references.reference != ""]  # pylint: disable=unsubscriptable-object
            .reset_index()[['document_id', 'source_id', 'reference']]
            .copy()
            .drop_duplicates()
        ).set_index(['document_id'], drop=True)
        page_references.drop(columns=['reference'], inplace=True)

        missing: tuple[str, str] = (MISSING_SPEAKER_NOTE.speaker_note_id, MISSING_SPEAKER_NOTE.speaker_note)
        speaker_notes_data: pd.DataFrame = (
            pd.DataFrame(
                itertools.chain(
                    [missing], ((x.speaker_note_id, x.speaker_note) for x in result.speaker_notes.values())
                ),
                columns=['speaker_note_id', 'speaker_note'],
            )
            .set_index('speaker_note_id')
            .fillna("")
        )

        protocol_ids: set[int] = set(utterances.document_id.unique())
        empty_protocols: pd.DataFrame = protocols[~protocols.index.isin(protocol_ids)]

        return {
            "protocols": protocols,
            "utterances": utterances,
            "page_references": page_references,
            "source_references": source_references,
            "speaker_notes": speaker_notes_data,
            "empty_protocols": empty_protocols,
        }


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

    def collect(self, filenames) -> CorpusIndexFactory:

        service: CorpusScanner = CorpusScanner(self.parser)
        self.data = service.to_dataframes(service.scan(filenames))
        return self

    def to_csv(self, folder: str) -> CorpusIndexFactory:
        if folder:
            os.makedirs(folder, exist_ok=True)

            for tablename, df in self.data.items():
                filename: str = jj(folder, f"{tablename}.csv.gz")
                df.to_csv(filename, sep=self.sep)

            logger.info("Corpus index: stored.")

        return self

    # def upload(self, *, db: database.DatabaseInterface, folder: str) -> CorpusIndexFactory:
    #     """Loads corpus indexes into given database."""

    #     with db:
    #         db.set_deferred(True)
    #         for cfg in self.schema.derived_tables:
    #             data: pd.DataFrame = (
    #                 self.data[cfg.tablename].reset_index()
    #                 if self.data
    #                 else pd.read_csv(jj(folder, cfg.basename), sep=cfg.sep, index_col=None)
    #             )
    #             db.store(data=data, tablename=cfg.tablename)

    #     return self

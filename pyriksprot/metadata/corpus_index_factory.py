from __future__ import annotations

import itertools
import os
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence, Type

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus.utility import (
    format_protocol_name,
    get_chamber_by_filename,
    load_chamber_indexes,
    ls_corpus_folder,
)
from pyriksprot.interface import MISSING_SPEAKER_NOTE, IProtocol, IProtocolParser, SpeakerNote, Speech
from pyriksprot.to_speech import to_speeches

from .schema import MetadataSchema

jj = os.path.join


_WORKER_PARSER: Any = None  # set in _init_worker


def _init_worker(parser_cls_or_obj: Any) -> None:
    """
    Create exactly one parser per worker process (important for performance).

    Note: If you pass a parser *instance*, it must be picklable to reach workers.
          Prefer passing a parser *class* to CorpusScanner(parser=YourParserClass).
    """
    global _WORKER_PARSER
    _WORKER_PARSER = parser_cls_or_obj() if isinstance(parser_cls_or_obj, type) else parser_cls_or_obj


def _scan_one(args: tuple[int, str, dict[str, str], bool]) -> tuple[
    tuple[int, str, str, int, str, str],  # protocol row
    list[tuple],  # utterances rows
    list[tuple],  # page refs rows
    dict[str, SpeakerNote],  # speaker notes
    list[Speech],  # speeches
]:
    document_id, filename, protocol_to_chamber, use_preface_name = args

    protocol: IProtocol = _WORKER_PARSER.parse(filename, use_preface_name=use_preface_name, ignore_tags={"teiHeader"})

    chamber_abbrev: str = protocol_to_chamber.get(protocol.name) or get_chamber_by_filename(filename)
    formatted_name: str = format_protocol_name(protocol.name, chamber_abbrev=chamber_abbrev)

    protocols_row: tuple[int, str, str, int, str, str] = (
        document_id,
        protocol.name,
        protocol.date,
        int(protocol.date[:4]),
        chamber_abbrev,
        formatted_name,
    )

    utterances_rows: list[tuple[int, str, str, str, int]] = [
        (document_id, u.u_id, u.who, u.speaker_note_id, u.page_number) for u in protocol.utterances
    ]

    page_refs_rows: list[tuple[int, int, int, str]] = [
        (document_id, p.source_id, p.page_number, p.reference) for p in protocol.page_references
    ]

    speeches: list[Speech] = to_speeches(protocol=protocol, merge_strategy="chain") or []
    speaker_notes: dict[str, SpeakerNote] = protocol.get_speaker_notes()

    return protocols_row, utterances_rows, page_refs_rows, speaker_notes, speeches


class CorpusScanner:
    @dataclass
    class ScanResult:
        protocols: list[tuple[int, str, str, str, str]] = field(default_factory=list)
        utterances: list[tuple] = field(default_factory=list)
        page_references: list[tuple] = field(default_factory=list)
        speaker_notes: dict[str, SpeakerNote] = field(default_factory=dict)
        speeches: list[Speech] = field(default_factory=list)

    def __init__(self, parser: IProtocolParser | Type[IProtocolParser]) -> None:
        self.parser: IProtocolParser | Type[IProtocolParser] = parser

    def scan_simple(
        self, filenames: Sequence[str], chambers: dict[str, set[str]], use_preface_name: bool = False
    ) -> CorpusScanner.ScanResult:
        protocol_to_chamber: dict[str, str] = {v: k for k, p in chambers.items() for v in p}

        data: CorpusScanner.ScanResult = CorpusScanner.ScanResult()
        # FIXME: #77 change: Add chamber_abbrev to the protocol index
        for document_id, filename in tqdm(enumerate(filenames)):
            protocol: IProtocol = self.parser.parse(
                filename, use_preface_name=use_preface_name, ignore_tags={"teiHeader"}
            )
            chamber_abbrev: str = protocol_to_chamber.get(protocol.name)
            if not chamber_abbrev:
                chamber_abbrev = get_chamber_by_filename(filename)

            formatted_name: str = format_protocol_name(protocol.name, chamber_abbrev=chamber_abbrev)

            data.protocols.append(
                (document_id, protocol.name, protocol.date, int(protocol.date[:4]), chamber_abbrev, formatted_name)
            )
            data.utterances.extend(
                tuple([document_id, u.u_id, u.who, u.speaker_note_id, u.page_number]) for u in protocol.utterances
            )
            data.page_references.extend(
                tuple([document_id, p.source_id, p.page_number, p.reference]) for p in protocol.page_references
            )
            data.speeches.extend(to_speeches(protocol=protocol, merge_strategy='chain') or [])

            data.speaker_notes.update(protocol.get_speaker_notes())

        return data

    def scan(
        self,
        filenames: Sequence[str],
        chambers: dict[str, set[str]],
        use_preface_name: bool = False,
        *,
        max_workers: Optional[int] = None,
        chunksize: int = 8,
    ) -> CorpusScanner.ScanResult:
        """
        Parallel version of scan() using multiple processes.

        Performance tips:
        - Prefer constructing CorpusScanner with a parser *class* (not an instance),
          so workers can instantiate locally and avoid pickling issues.
        - Tune chunksize: higher reduces overhead, too high can reduce load balancing.
        """
        protocol_to_chamber: dict[str, str] = {v: k for k, p in chambers.items() for v in p}

        if max_workers is None:
            cpu = os.cpu_count() or 1
            max_workers = max(1, cpu - 1)

        data: CorpusScanner.ScanResult = CorpusScanner.ScanResult()

        # Pack per-file args once (keeps map fast)
        work_items: list[tuple[int, str, dict[str, str], bool]] = [
            (document_id, filename, protocol_to_chamber, use_preface_name)
            for document_id, filename in enumerate(filenames)
        ]

        with ProcessPoolExecutor(max_workers=max_workers, initializer=_init_worker, initargs=(self.parser,)) as ex:
            # executor.map is typically faster than submit/as_completed for big batches
            for protocols_row, utter_rows, page_rows, notes, speeches in tqdm(
                ex.map(_scan_one, work_items, chunksize=chunksize),
                total=len(work_items),
            ):
                data.protocols.append(protocols_row)
                data.utterances.extend(utter_rows)
                data.page_references.extend(page_rows)
                data.speaker_notes.update(notes)
                data.speeches.extend(speeches)

        # Deterministic ordering (map preserves order, but keeping this is harmless)
        data.protocols.sort(key=lambda r: r[0])

        return data

    def to_dataframes(self, result: CorpusScanner.ScanResult) -> dict[str, pd.DataFrame]:
        """Store enough source reference data to reconstruct source urls."""

        protocols: pd.DataFrame = pd.DataFrame(
            data=result.protocols,
            columns=['document_id', 'document_name', 'date', 'year', 'chamber_abbrev', 'protocol_name'],
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
        speeches: pd.DataFrame = (
            pd.DataFrame(
                itertools.chain(
                    (
                        (
                            x.speech_id,
                            x.who,
                            x.get_year(),
                            x.document_name,
                            x.protocol_name,
                            x.speech_index,
                            x.page_number,
                            x.num_tokens,
                            x.num_words,
                        )
                        for x in result.speeches
                    )
                ),
                columns=[
                    'speech_id',
                    'person_id',
                    'year',
                    'document_name',
                    'protocol_name',
                    'speech_index',
                    'page_number',
                    'num_tokens',
                    'num_words',
                ],
            )
            .set_index('speech_id')
            .fillna("")
        )
        protocol_ids: set[int] = set(utterances.document_id.unique())
        empty_protocols: pd.DataFrame = protocols[~protocols.index.isin(protocol_ids)]

        return {
            "protocols": protocols,
            "utterances": utterances,
            "speeches": speeches,
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
        chambers: dict[str, set[str]] = load_chamber_indexes(corpus_folder)

        return self.collect(filenames, chambers).to_csv(target_folder)

    def collect(self, filenames: list[str], chambers: dict[str, set[str]]) -> CorpusIndexFactory:
        service: CorpusScanner = CorpusScanner(self.parser)
        scan_result: CorpusScanner.ScanResult = service.scan(filenames, chambers)
        self.data = service.to_dataframes(scan_result)
        return self

    def to_csv(self, folder: str) -> CorpusIndexFactory:
        if folder:
            os.makedirs(folder, exist_ok=True)

            for tablename, df in self.data.items():
                filename: str = jj(folder, f"{tablename}.csv.gz")
                df.to_csv(filename, sep=self.sep, compression=dict(method='gzip', mtime=0))

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

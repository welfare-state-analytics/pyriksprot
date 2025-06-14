from __future__ import annotations

import os
import typing as t

import pandas as pd
from loguru import logger
from tqdm import tqdm

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import to_speech
from pyriksprot.corpus import corpus_index, iterate, tagged

jj = os.path.join
relpath = os.path.relpath

# pylint: disable=too-many-arguments


def extract_speech_index(
    *,
    source_folder: str,
    metadata_filename: str,
    target_name: str = 'speech_index.csv.gz',
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    years: str = None,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    merge_strategy: to_speech.MergeStrategyType = 'chain',
    source_pattern: str = '**/prot-*.zip',
) -> pd.DataFrame:
    """Generates a speech index for corpus in `source_folder`, and according to given parameters..

    Args:
        source_folder (str): Source folder
        metadata_filename (str): Metadata database filename (Sqlite3)
        target_names (str): Target filename(s).
        segment_level (interface.SegmentLevel, optional): Document level. Defaults to None.
        segment_skip_size (int, optional): Size of text to include. Defaults to 1.
        years (str, optional): Years filter. Defaults to None.
        multiproc_keep_order (str, optional): Multiprocessing option. Defaults to None.
        multiproc_processes (int, optional): Number of processes. Defaults to 1.
        multiproc_chunksize (int, optional): Size of work loads assigned to each process. Defaults to 100.
        merge_strategy (to_speech.MergeStrategyType, optional): Speech merge strategy. Defaults to 'chain'.
    """
    logger.info("extracting corpus speech index...")

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern=source_pattern, years=years
    )
    logger.info("loading parliamentary metadata...")

    speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=metadata_filename)

    def get_speaker(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)

    preprocess: t.Callable[[iterate.ProtocolSegment], None] = (
        get_speaker if segment_level not in ('protocol', None) else None
    )

    speeches: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=segment_level,
        segment_skip_size=segment_skip_size,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_processes=multiproc_processes,
        multiproc_chunksize=multiproc_chunksize,
        merge_strategy=merge_strategy,
        preprocess=preprocess,
    )

    df: pd.DataFrame = pd.DataFrame(data=(speech.to_dict() for speech in tqdm(speeches)))

    if not target_name.endswith('feather'):
        df.to_csv(target_name, sep='\t')

    df.to_feather(f"{target_name.rstrip('.gz').rstrip('.zip').rstrip('.csv').rstrip('.feather')}.feather")

    return df

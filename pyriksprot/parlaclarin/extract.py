from __future__ import annotations

import os
from typing import Sequence

from loguru import logger

from pyriksprot.segment import ProtocolSegment

from .. import corpus_index, dehyphenation, dispatch, interface, merge_segments
from .. import metadata as md
from .. import utility
from . import iterate

# pylint: disable=too-many-arguments


def create_dehyphen(data_path: str) -> dehyphenation.SwedishDehyphenatorService:
    opts = dict(
        word_frequency_filename=os.path.join(data_path, 'riksdagen-corpus-term-frequencies.pkl'),
        whitelist_filename=os.path.join(data_path, 'dehyphen_whitelist.txt.gz'),
        whitelist_log_filename=os.path.join(data_path, 'dehyphen_whitelist_log.pkl'),
        unresolved_filename=os.path.join(data_path, 'dehyphen_unresolved.txt.gz'),
    )
    return dehyphenation.SwedishDehyphenatorService.create_dehypen(*opts)


def extract_corpus_text(
    source_folder: str = None,
    metadata_filename: str = None,
    target_name: str = None,
    target_type: str = None,
    segment_level: interface.SegmentLevel = None,
    temporal_key: interface.TemporalKey = None,
    group_keys: Sequence[interface.GroupingKey] = None,
    years: str = None,
    segment_skip_size: int = 1,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    dedent: bool = True,
    dehyphen: bool = False,
    data_path: str = '.',
    compress_type: dispatch.CompressType = dispatch.CompressType.Zip,
    **_,
) -> None:
    """Group extracted protocol blocks by `temporal_key` and attribute `group_keys`.

    Temporal key kan be any of None, 'Year', 'Lustrum', 'Decade' or custom year periods
    - 'Year', 'Lustrum', 'Decade' or custom year periods given as comma separated string


    Args:
        source_folder (str, optional): Corpus source folder. Defaults to None.
        target_name (str, optional): Target name. Defaults to None.
        target_type (str, optional): Target store type. Defaults to None.
        segment_level (interface.SegmentLevel, optional): Level of protocol segments yielded by iterator. Defaults to None.
        segment_skip_size (int, optional): Segment skip size. Defaults to 1.
        group_temporal_key (str, optional): Temporal grouping key used in merge. Defaults to None.
        group_keys (Sequence[str], optional): Other grouping keys. Defaults to None.
        years (str, optional): Years filter. Defaults to None.
        multiproc_keep_order (str, optional): Force correct iterate yield order when multiprocessing. Defaults to None.
        multiproc_processes (int, optional): Number of processes during iterate. Defaults to 1.
        multiproc_chunksize (int, optional): Chunksize to use per process during iterate. Defaults to 100.
        dedent (bool, optional): Dedent text. Defaults to True.
        dehyphen (bool, optional): Dehyphen text. Defaults to False.
        data_path (str, optional): Path to model data (used by dedent/dehyphen). Defaults to '.'.
    """
    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.xml', years=years
    )

    dehypenator = create_dehyphen(data_path) if dehyphen else None

    get_speaker = None
    if segment_level == interface.SegmentLevel.Speech:
        speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=metadata_filename)
        get_speaker = speaker_service.get_speaker_info

    def preprocess(item: ProtocolSegment) -> None:

        if dedent:
            item.data = utility.dedent(item.data)

        if dehypenator:
            item.data = dehypenator(item.data)  # pylint: disable=not-callable

        if get_speaker:
            item.speaker_info = get_speaker(item.u_id, item.who, item.year)

    segments: iterate.XmlUntangleSegmentIterator = iterate.XmlUntangleSegmentIterator(
        filenames=source_index.paths,
        segment_level=segment_level,
        segment_skip_size=segment_skip_size,
        multiproc_processes=multiproc_processes,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_chunksize=multiproc_chunksize,
        preprocess=preprocess,
    )

    merger: merge_segments.SegmentMerger = merge_segments.SegmentMerger(
        source_index=source_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    with dispatch.IDispatcher.dispatcher(target_type)(target_name, compress_type=compress_type) as dispatcher:
        for item in merger.merge(segments):
            dispatcher.dispatch(list(item.values()))

    # metadata_index.store(target_name if isdir(target_name) else dirname(target_name))

    logger.info(f"Corpus stored in {target_name}.")

from __future__ import annotations

import shutil
import typing as t
from os.path import isdir

from loguru import logger
from tqdm import tqdm

from pyriksprot.corpus import iterate, tagged
from pyriksprot.dispatch.item import DispatchItem

from .. import interface
from .. import metadata as md
from .. import to_speech
from ..corpus import corpus_index
from ..dispatch import dispatch, merge

# pylint: disable=too-many-arguments, W0613


def extract_corpus_tags(
    *,
    source_folder: str,
    metadata_filename: str,
    target_name: str,
    content_type: interface.ContentType = interface.ContentType.TaggedFrame,
    target_type: dispatch.TargetTypeKey = None,
    compress_type: dispatch.CompressType = dispatch.CompressType.Lzma,
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    years: str = None,
    temporal_key: interface.TemporalKey = None,
    group_keys: t.Sequence[interface.GroupingKey] = None,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    merge_strategy: to_speech.MergeStrategyType = 'chain',
    force: bool = False,
    skip_lemma: bool = False,
    skip_text: bool = False,
    skip_puncts: bool = False,
    skip_stopwords: bool = False,
    lowercase: bool = True,
    progress: bool = True,
    source_pattern: str = '**/prot-*.zip',
) -> None:
    """Group extracted protocol blocks by `temporal_key` and attribute `group_keys`.

    Temporal key kan be any of None, 'Year', 'Lustrum', 'Decade' or custom year periods
    - 'Year', 'Lustrum', 'Decade' or custom year periods given as comma separated string


    Args:
        source_folder (str, optional): Corpus source folder. Defaults to None.
        target_name (str, optional): Target name. Defaults to None.
        content_type (interface.ContentType): Content type to yield (text or tagged_text)
        target_type (str, optional): Target store type. Defaults to None.
        segment_level (interface.SegmentLevel, optional): Level of protocol segments yielded by iterator. Defaults to None.
        segment_skip_size (int, optional): Segment skip size. Defaults to 1.
        group_temporal_key (str, optional): Temporal grouping key used in merge. Defaults to None.
        group_keys (Sequence[str], optional): Other grouping keys. Defaults to None.
        years (str, optional): Years filter. Defaults to None.
        multiproc_keep_order (str, optional): Force correct iterate yield order when multiprocessing. Defaults to None.
        multiproc_processes (int, optional): Number of processes during iterate. Defaults to 1.
        multiproc_chunksize (int, optional): Chunksize to use per process during iterate. Defaults to 100.
        force (bool, optional): Clear target if it exists. Defaults to False
        skip_lemma (bool, optional): Defaults to False
        skip_text (bool, optional): Defaults to False
        lowercase (bool, optional): Defaults to False
    """
    logger.info("extracting tagged corpus...")

    if isdir(target_name):
        if force:
            shutil.rmtree(target_name, ignore_errors=True)
        else:
            raise ValueError(f"target {target_name} exists (use --force to override")

    dispatch_opts: dict = {
        'lowercase': lowercase,
    }

    if skip_lemma or skip_text:
        if target_type not in (
            'single-tagged-frame-per-group',
            'single-id-tagged-frame-per-group',
            'single-id-tagged-frame',
        ):
            raise ValueError(f"lemma/text skip not implemented for {target_type}")
        dispatch_opts = {
            **dispatch_opts,
            **dict(
                skip_lemma=skip_lemma,
                skip_text=skip_text,
                skip_puncts=skip_puncts,
                skip_stopwords=skip_stopwords,
            ),
        }

    lookups: md.Codecs = md.Codecs().load(metadata_filename)

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern=source_pattern, years=years
    )
    # logger.info("loading parliamentary metadata...")

    # FIXME: How to ensure metadata tag is the same as corpus??? Add tag to DB?
    speaker_service: md.SpeakerInfoService = md.SpeakerInfoService(database_filename=metadata_filename)

    def get_speaker(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)

    preprocess: t.Callable[[iterate.ProtocolSegment], None] = (
        get_speaker if segment_level not in ('protocol', None) else None
    )
    texts: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        segment_skip_size=segment_skip_size,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_processes=multiproc_processes,
        multiproc_chunksize=multiproc_chunksize,
        merge_strategy=merge_strategy,
        preprocess=preprocess,
    )

    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=source_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    with dispatch.IDispatcher.dispatcher(target_type)(
        target_name=target_name, compress_type=compress_type, lookups=lookups, **dispatch_opts
    ) as dispatcher:
        data: t.Iterable[dict[str, DispatchItem]]
        n_total: int = len(source_index.source_items)

        for data in tqdm(merger.merge(texts), total=n_total, miniters=10, disable=not progress):
            if not data:
                logger.error("merge returned empty data")
                continue

            # items: list[DispatchItem] = list(data.values())
            # print(f"dispatch: group count is {len(items)}")
            # for item in items:
            #     print(f"   item: filename={item.filename} temporal={item.group_temporal_value} level={item.segment_level}")
            #     for segment in item.protocol_segments:
            #         print(f"         segment: filename={segment.filename} temporal={segment.protocol_name} speaker={segment.speaker_info.person_id if segment.speaker_info else 'missing'}")

            dispatcher.dispatch(list(data.values()))

    # metadata_index.store(target_name=target_name if isdir(target_name) else dirname(target_name))

    logger.info(f"Corpus stored in {target_name}.")
    logger.info(f"Please copy a corpus config `corpus.yml` to {target_name}.")

    # FIXME: #69 Write corpus config to file to target folder

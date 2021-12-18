from __future__ import annotations

import shutil
from os.path import dirname, isdir, join
from typing import Sequence

from loguru import logger
from tqdm import tqdm

from .. import corpus_index, dispatch, interface, member, merge
from . import iterate

# pylint: disable=too-many-arguments, W0613


def extract_corpus_tags(
    source_folder: str = None,
    target_name: str = None,
    content_type: interface.ContentType = interface.ContentType.TaggedFrame,
    target_type: str = None,
    compress_type: dispatch.CompressType = dispatch.CompressType.Lzma,
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    years: str = None,
    temporal_key: interface.TemporalKey = None,
    group_keys: Sequence[interface.GroupingKey] = None,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    speech_merge_strategy: interface.MergeSpeechStrategyType = 'who_sequence',
    force: bool = False,
    skip_lemma: bool = False,
    skip_text: bool = False,
    skip_puncts: bool = False,
    skip_stopwords: bool = False,
    lowercase: bool = True,
    progress: bool = True,
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
    logger.info("creating index over corpus source item...")

    if isdir(target_name):
        if force:
            shutil.rmtree(target_name, ignore_errors=True)
        else:
            raise ValueError(f"target {target_name} exists (use --force to override")

    dispatch_opts: dict = {
        'lowercase': lowercase,
    }

    if skip_lemma or skip_text:
        if target_type not in ('single-tagged-frame-per-group', 'single-id-tagged-frame-per-group'):
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

    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=years
    )
    logger.info("loading index over parliamentary persons...")
    member_index: member.ParliamentaryMemberIndex = member.ParliamentaryMemberIndex(source=source_folder, tag=None)
    texts: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        segment_skip_size=segment_skip_size,
        multiproc_keep_order=multiproc_keep_order,
        multiproc_processes=multiproc_processes,
        multiproc_chunksize=multiproc_chunksize,
        speech_merge_strategy=speech_merge_strategy,
        preprocessor=None,
    )

    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=source_index,
        member_index=member_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    with dispatch.IDispatcher.dispatcher(target_type)(
        target_name=target_name, compress_type=compress_type, **dispatch_opts
    ) as dispatcher:

        n_total: int = len(source_index.source_items)

        for item in tqdm(merger.merge(texts), total=n_total, miniters=10, disable=not progress):
            if not item:
                logger.error("merge returned empty data")
                continue
            dispatcher.dispatch(list(item.values()))

    member_index.to_dataframe().to_json(join(dirname(target_name), 'person_index.json'))

    print(f"Corpus stored in {target_name}.")

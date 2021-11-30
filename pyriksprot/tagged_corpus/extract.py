from __future__ import annotations

from typing import Sequence

from .. import corpus_index, dispatch, interface, member, merge
from . import iterate

# pylint: disable=too-many-arguments, W0613


def extract_corpus_tags(
    source_folder: str = None,
    target_name: str = None,
    content_type: interface.ContentType = interface.ContentType.TaggedFrame,
    target_type: dispatch.TargetType = None,
    segment_level: interface.SegmentLevel = None,
    segment_skip_size: int = 1,
    years: str = None,
    temporal_key: interface.TemporalKey = None,
    group_keys: Sequence[interface.GroupingKey] = None,
    multiproc_keep_order: str = None,
    multiproc_processes: int = 1,
    multiproc_chunksize: int = 100,
    speech_merge_strategy: interface.MergeSpeechStrategyType = 'who_sequence',
    **_,
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
    """
    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=source_folder, source_pattern='**/prot-*.zip', years=years
    )
    member_index: member.ParliamentaryMemberIndex = member.ParliamentaryMemberIndex(
        f'{source_folder}/members_of_parliament.csv'
    )
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

    with dispatch.IDispatcher.get_cls(target_type)(target_type=target_type, target_name=target_name) as dispatcher:
        for item in merger.merge(texts):
            dispatcher.dispatch(item)

    print(f"Corpus stored in {target_name}.")

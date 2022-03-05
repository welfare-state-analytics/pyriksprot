from typing import List

import pytest

from pyriksprot import corpus_index, interface, merge
from pyriksprot import metadata as md
from pyriksprot.tagged_corpus import iterate, persist

from .utility import TAGGED_METADATA_DATABASE_NAME, TAGGED_SOURCE_FOLDER

# pylint: disable=unused-variable, redefined-outer-name


@pytest.fixture
def source_index() -> corpus_index.CorpusSourceIndex:
    return corpus_index.CorpusSourceIndex.load(
        source_folder=TAGGED_SOURCE_FOLDER, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )


@pytest.fixture
def metadata_index() -> md.MetaDataIndex:
    return md.MetaDataIndex.load(database_filename=TAGGED_METADATA_DATABASE_NAME)


@pytest.fixture
def utterance_segments(source_index) -> List[interface.ProtocolSegment]:
    """Iterate protocols at lowest prossible level that has tagged text (utterance)"""
    content_type: interface.ContentType = interface.ContentType.TaggedFrame
    segment_level: interface.SegmentLevel = interface.SegmentLevel.Utterance
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        speech_merge_strategy=None,
    )
    segments = list(segments)
    return segments


def test_segment_merger_merge_on_protocol_level_group_by_who(metadata_index, source_index, utterance_segments):

    """Load source protocols to simplify tests"""
    protocols: List[interface.Protocol] = list(persist.load_protocols(source=TAGGED_SOURCE_FOLDER))

    """Check that iterator yields all utterances"""
    assert len(utterance_segments) == sum(map(len, protocols))

    """Iterate at protocol level with no temporal key gives one group per docoment"""
    temporal_key: interface.TemporalKey = None
    group_keys: List[interface.GroupingKey] = [interface.GroupingKey.Who]
    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=source_index,
        metadata_index=metadata_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    groups = merger.merge(utterance_segments)

    assert sum([len(g.values()) for g in groups]) == sum([len(set(u.who for u in p.utterances)) for p in protocols])

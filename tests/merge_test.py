from typing import List

import pytest

from pyriksprot import corpus_index, interface, merge_segments, segment
from pyriksprot.tagged_corpus import iterate, persist

from .utility import TAGGED_SOURCE_FOLDER

# pylint: disable=unused-variable, redefined-outer-name


@pytest.fixture
def protocol_segments(source_index: corpus_index.CorpusSourceIndex) -> List[segment.ProtocolSegment]:
    """Iterate protocols at lowest prossible level that has tagged text (utterance)"""
    content_type: interface.ContentType = interface.ContentType.TaggedFrame
    segment_level: interface.SegmentLevel = interface.SegmentLevel.Utterance
    segments: segment.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        merge_strategy=None,
    )
    segments = list(segments)
    return segments


def test_segment_merger_merge_on_protocol_level_group_by_who(
    source_index: corpus_index.CorpusSourceIndex,
    protocol_segments: List[segment.ProtocolSegment],
):

    """Load source protocols to simplify tests"""
    protocols: List[interface.Protocol] = list(persist.load_protocols(source=TAGGED_SOURCE_FOLDER))

    """Check that iterator yields all utterances"""
    assert len(protocol_segments) == sum(map(len, protocols))

    """Iterate at protocol level with no temporal key gives one group per docoment"""
    temporal_key: interface.TemporalKey = None
    group_keys: List[interface.GroupingKey] = [interface.GroupingKey.who]
    merger: merge_segments.SegmentMerger = merge_segments.SegmentMerger(
        source_index=source_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    groups = merger.merge(protocol_segments)

    assert sum([len(g.values()) for g in groups]) == sum([len(set(u.who for u in p.utterances)) for p in protocols])

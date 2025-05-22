from typing import List

import pytest

from pyriksprot import interface
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus import corpus_index, iterate, tagged
from pyriksprot.dispatch import merge
from pyriksprot.to_speech import MergeByChain

from .utility import sample_tagged_frames_corpus_exists

# pylint: disable=unused-variable, redefined-outer-name


@pytest.fixture
def protocol_segments(source_index: corpus_index.CorpusSourceIndex) -> List[iterate.ProtocolSegment]:
    """Iterate protocols at lowest prossible level that has tagged text (utterance)"""
    content_type: interface.ContentType = interface.ContentType.TaggedFrame
    segment_level: interface.SegmentLevel = interface.SegmentLevel.Utterance
    segments: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=content_type,
        segment_level=segment_level,
        merge_strategy=None,
    )
    return list(segments)


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_segment_merger_merge_on_protocol_level_group_by_who(
    source_index: corpus_index.CorpusSourceIndex,
    protocol_segments: List[iterate.ProtocolSegment],
):
    tagged_folder: str = ConfigStore.config().get("tagged_frames.folder")

    """Load source protocols to simplify tests"""
    protocols: List[interface.Protocol] = list(tagged.load_protocols(source=tagged_folder))

    """Check that iterator yields all utterances"""
    assert len(protocol_segments) == sum(map(len, protocols))

    """Iterate at protocol level with no temporal key gives one group per docoment"""
    temporal_key: interface.TemporalKey = None
    group_keys: List[interface.GroupingKey] = [interface.GroupingKey.who]
    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=source_index,
        temporal_key=temporal_key,
        grouping_keys=group_keys,
    )

    groups = merger.merge(protocol_segments)

    assert sum(len(g.values()) for g in groups) == sum(len(set(u.who for u in p.utterances)) for p in protocols)


def test_merge_by_chain():
    U = interface.Utterance

    # Two utterances with no chain should not be merged
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id=None, who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id=None, next_id=None, who="B", speaker_note_id="N2"),
    ]

    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )

    assert [set(x.u_id for x in s) for s in speeches] == [{"1"}, {"2"}]

    # Two utterances with chain but different speaker_note_id should raise ValueError
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id="2", who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id="1", next_id=None, who="unknown", speaker_note_id="N2"),
    ]

    with pytest.raises(ValueError):
        speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
            utterances, merge_consecutive_unknowns=True
        )

    # Two utterances with chain and same speaker_note_id should be merged
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id="2", who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id="1", next_id=None, who="A", speaker_note_id="N1"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 1 and len(speeches[0]) == 2

    # Three utterances with chain and same speaker_note_id should be merged
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id="2", who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id="1", next_id="3", who="A", speaker_note_id="N1"),
        U(u_id="3", prev_id="2", next_id=None, who="A", speaker_note_id="N1"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 1 and len(speeches[0]) == 3

    # Two chains of utterences should not be merged
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id="2", who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id="1", next_id=None, who="A", speaker_note_id="N1"),
        U(u_id="3", prev_id=None, next_id="4", who="B", speaker_note_id="N2"),
        U(u_id="4", prev_id="3", next_id=None, who="B", speaker_note_id="N2"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 2 and len(speeches[0]) == 2 and len(speeches[1]) == 2

    # One chain of utterences, followeded by one utterance with no chain, should not be merged
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id="2", who="A", speaker_note_id="N1"),
        U(u_id="2", prev_id="1", next_id=None, who="A", speaker_note_id="N1"),
        U(u_id="3", prev_id=None, next_id=None, who="B", speaker_note_id="N2"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 2 and len(speeches[0]) == 2 and len(speeches[1]) == 1

    # Utterances should be merged with previous utterance if who is "unknown" and merge_consecutive_unknowns is True and speaker_note_id is the same
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id=None, who="unknown", speaker_note_id="N1"),
        U(u_id="2", prev_id=None, next_id=None, who="unknown", speaker_note_id="N1"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 1 and len(speeches[0]) == 2

    # Utterances should not be merged with previous utterance if who is "unknown" and merge_consecutive_unknowns is False and speaker_note_id is the same
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id=None, who="unknown", speaker_note_id="N1"),
        U(u_id="2", prev_id=None, next_id=None, who="unknown", speaker_note_id="N1"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=False
    )
    assert len(speeches) == 2 and len(speeches[0]) == 1 and len(speeches[1]) == 1

    # Utterances should not be merged with previous utterance if who is "unknown" and merge_consecutive_unknowns is True and speaker_note_id is different
    utterances: list[interface.Utterance] = [
        U(u_id="1", prev_id=None, next_id=None, who="unknown", speaker_note_id="N1"),
        U(u_id="2", prev_id=None, next_id=None, who="unknown", speaker_note_id="N2"),
    ]
    speeches: list[list[interface.Utterance]] = MergeByChain.group_utterances_by_chain(
        utterances, merge_consecutive_unknowns=True
    )
    assert len(speeches) == 2 and len(speeches[0]) == 1 and len(speeches[1]) == 1

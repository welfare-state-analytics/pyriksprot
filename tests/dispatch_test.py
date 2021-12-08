import os
import uuid
from typing import Mapping

import pytest

from pyriksprot import corpus_index, dispatch, interface, member, merge
from pyriksprot.tagged_corpus import iterate

# pylint: disable=unused-variable, redefined-outer-name

SOURCE_FOLDER: str = './tests/test_data/tagged'


@pytest.fixture
def source_index() -> corpus_index.CorpusSourceIndex:
    return corpus_index.CorpusSourceIndex.load(
        source_folder=SOURCE_FOLDER, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(f'{SOURCE_FOLDER}/members_of_parliament.csv')


@pytest.fixture
def tagged_protocol_level_groups(
    source_index: corpus_index.CorpusSourceIndex,
) -> Mapping[str, merge.MergedSegmentGroup]:
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Protocol,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        segment_skip_size=1,
    )
    groups = merge.SegmentMerger(
        source_index=source_index, member_index=member_index, temporal_key=None, grouping_keys=None
    ).merge(segments)
    return groups


def test_folder_with_zips_dispatch(tagged_protocol_level_groups):
    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.FolderDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Gzip,
    ) as dispatcher:
        for group in tagged_protocol_level_groups:
            dispatcher.dispatch(list(group.values()))
    assert os.path.isdir(target_name)


def test_zip_file_dispatch(tagged_protocol_level_groups):
    target_name: str = f'./tests/output/{uuid.uuid1()}.zip'
    target_name: str = './tests/output/APA.zip'
    with dispatch.ZipFileDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
    ) as dispatcher:
        for group in tagged_protocol_level_groups:
            dispatcher.dispatch(list(group.values()))
    assert os.path.isfile(target_name)

import glob
import os
import uuid
from typing import List, Mapping, Set, Type

import pytest

from pyriksprot import corpus_index, dispatch, interface, member, merge, utility
from pyriksprot.tagged_corpus import iterate

# pylint: disable=unused-variable, redefined-outer-name

SOURCE_FOLDER: str = './tests/test_data/tagged'


@pytest.fixture
def source_index() -> corpus_index.CorpusSourceIndex:
    return corpus_index.CorpusSourceIndex.load(
        source_folder=SOURCE_FOLDER, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )


@pytest.fixture
def tagged_speeches(
    source_index: corpus_index.CorpusSourceIndex,
    member_index: member.ParliamentaryMemberIndex,
) -> Mapping[str, merge.MergedSegmentGroup]:
    segments: interface.ProtocolSegmentIterator = iterate.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        speech_merge_strategy=interface.MergeSpeechStrategyType.WhoSequence,
        segment_skip_size=1,
    )
    groups = merge.SegmentMerger(
        source_index=source_index, member_index=member_index, temporal_key=None, grouping_keys=None
    ).merge(segments)
    return groups


def test_folder_with_zips_dispatch(tagged_speeches):
    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.FolderDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Gzip,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert os.path.isdir(target_name)


def test_zip_file_dispatch(tagged_speeches):
    target_name: str = f'./tests/output/{uuid.uuid1()}.zip'
    with dispatch.ZipFileDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert os.path.isfile(target_name)


def test_find_dispatch_class():
    classes: List[Type] = dispatch.IDispatcher.dispatchers()
    assert len(classes) > 0

    dispatch_keys: Set[str] = {x.name for x in classes}

    assert len(dispatch_keys) > 0

    expected_keys: Set[str] = {'checkpoint', 'feather', 'zip', 'plain'}
    assert expected_keys.intersection(dispatch_keys) == expected_keys


def test_checkpoint_dispatch(tagged_speeches, source_index: corpus_index.CorpusSourceIndex):

    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.CheckpointDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert os.path.isdir(target_name)

    files_on_disk: Set[str] = set(
        os.path.basename(x) for x in glob.glob(os.path.join(target_name, '**/prot-*.zip'), recursive=True)
    )
    files_in_index: Set[str] = set(source_index.filenames)
    assert (files_in_index - files_on_disk) == set()


def test_feather_dispatch(tagged_speeches, source_index: corpus_index.CorpusSourceIndex):

    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.FeatherDispatcher(
        target_name=target_name,
        target_type=dispatch.TargetType.Checkpoint,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert os.path.isdir(target_name)

    files_on_disk: Set[str] = set(
        utility.strip_extensions(
            os.path.basename(x) for x in glob.glob(os.path.join(target_name, '**/prot-*.feather'), recursive=True)
        )
    )
    files_in_index: Set[str] = set(utility.strip_extensions(source_index.filenames))

    assert (files_in_index - files_on_disk) == set()
    assert os.path.isfile(os.path.join(target_name, 'document_index.feather'))

import glob
import uuid
from os.path import basename, isdir, isfile, join
from typing import List, Mapping, Set, Type

import pandas as pd
import pytest

from pyriksprot import cluster, collect_generic, corpus_index, dispatch, interface
from pyriksprot import metadata as md
from pyriksprot import utility
from pyriksprot.corpus import iterate, tagged

# pylint: disable=unused-variable, redefined-outer-name


@pytest.fixture
def tagged_speeches(
    source_index: corpus_index.CorpusSourceIndex,
    speaker_service: md.SpeakerInfoService,
) -> Mapping[str, collect_generic.ProtocolSegmentGroup]:
    def assign_speaker_info(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id)

    segments: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=cluster.MergeStrategyType.who_speaker_hash_sequence,
        segment_skip_size=1,
        preprocess=assign_speaker_info,
    )
    groups = collect_generic.SegmentMerger(source_index=source_index, temporal_key=None, grouping_keys=None).merge(
        segments
    )
    groups = list(groups)
    return groups


def test_folder_with_zips_dispatch(tagged_speeches: Mapping[str, collect_generic.ProtocolSegmentGroup]):
    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.FilesInFolderDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Plain,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert isdir(target_name)


def test_zip_file_dispatch(tagged_speeches: Mapping[str, collect_generic.ProtocolSegmentGroup]):
    target_name: str = f'./tests/output/{uuid.uuid1()}.zip'
    with dispatch.FilesInZipDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Zip,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert isfile(target_name)


def test_find_dispatch_class():
    classes: List[Type] = dispatch.IDispatcher.dispatchers()
    assert len(classes) > 0

    dispatch_keys: Set[str] = {x.name for x in classes}

    assert len(dispatch_keys) > 0

    expected_keys: Set[str] = {
        'files-in-zip',
        'single-tagged-frame-per-group',
        'single-id-tagged-frame-per-group',
        'single-id-tagged-frame',
        'checkpoint-per-group',
        'files-in-folder',
    }
    assert expected_keys.intersection(dispatch_keys) == expected_keys


def test_checkpoint_dispatch(
    tagged_speeches: Mapping[str, collect_generic.ProtocolSegmentGroup], source_index: corpus_index.CorpusSourceIndex
):

    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with dispatch.CheckpointPerGroupDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Zip,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert isdir(target_name)

    files_on_disk: Set[str] = set(basename(x) for x in glob.glob(join(target_name, '**/prot-*.zip'), recursive=True))
    files_in_index: Set[str] = set(source_index.filenames)
    assert (files_in_index - files_on_disk) == set()


def files_in_folder(folder: str, *, pattern: str, strip_path: bool = True, strip_ext: bool = True) -> Set[str]:
    files: List[str] = set(basename(x) for x in glob.glob(join(folder, pattern), recursive=True))
    if strip_path:
        files = {basename(x) for x in files}
    if strip_ext:
        files = {utility.strip_extensions(x) for x in files}
    return files


@pytest.mark.parametrize('cls', [dispatch.TaggedFramePerGroupDispatcher, dispatch.IdTaggedFramePerGroupDispatcher])
def test_single_feather_per_group_dispatch(
    tagged_speeches: Mapping[str, collect_generic.ProtocolSegmentGroup],
    source_index: corpus_index.CorpusSourceIndex,
    cls: Type[dispatch.IDispatcher],
):

    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with cls(
        target_name=target_name,
        compress_type=dispatch.CompressType.Feather,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert isdir(target_name)

    files_on_disk: Set[str] = files_in_folder(target_name, pattern='**/prot-*.feather')
    files_in_index: Set[str] = set(utility.strip_extensions(source_index.filenames))

    assert (files_in_index - files_on_disk) == set()
    assert isfile(join(target_name, 'document_index.feather'))


@pytest.mark.parametrize('cls', [dispatch.TaggedFramePerGroupDispatcher, dispatch.IdTaggedFramePerGroupDispatcher])
def test_single_feather_per_group_dispatch_with_skips(
    tagged_speeches: Mapping[str, collect_generic.ProtocolSegmentGroup],
    source_index: corpus_index.CorpusSourceIndex,
    cls: Type[dispatch.IDispatcher],
):

    target_name: str = f'./tests/output/{uuid.uuid1()}'
    with cls(
        target_name=target_name, compress_type=dispatch.CompressType.Feather, lowercase=True, skip_text=True
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert isdir(target_name)

    files_on_disk: Set[str] = files_in_folder(target_name, pattern='**/prot-*.feather')
    files_in_index: Set[str] = set(utility.strip_extensions(source_index.filenames))
    assert (files_in_index - files_on_disk) == set()

    """Assert that token column has been dropped"""
    any_basename: str = utility.strip_extensions(source_index.filenames[0])
    any_filename: str = join(target_name, any_basename.split('-')[1], f"{any_basename}.feather")

    tagged_frame: pd.DataFrame = pd.read_feather(any_filename)

    assert 'token_id' not in tagged_frame.columns
    assert 'token' not in tagged_frame.columns

    assert True

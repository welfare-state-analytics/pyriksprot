import glob
import uuid
from os.path import basename, isdir, isfile, join
from typing import List, Set, Type

import pandas as pd
import pytest

from pyriksprot import metadata as md
from pyriksprot import utility
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus import corpus_index
from pyriksprot.dispatch import dispatch
from pyriksprot.dispatch import merge as sg

from .utility import sample_tagged_frames_corpus_exists

# pylint: disable=unused-variable, redefined-outer-name


def test_find_dispatchers():
    target_type: dispatch.TargetTypeKey = 'single-id-tagged-frame'
    dispatcher: dispatch.IDispatcher = dispatch.IDispatcher.dispatcher(target_type)
    assert dispatcher is not None


def test_folder_with_zips_dispatch(tagged_speeches: list[dict[str, sg.DispatchItem]]):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}'
    lookups: md.Codecs = md.Codecs().load(database)
    with dispatch.FilesInFolderDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Plain,
        lookups=lookups,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert isdir(target_name)


def test_zip_file_dispatch(tagged_speeches: list[dict[str, sg.DispatchItem]]):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}.zip'
    lookups: md.Codecs = md.Codecs().load(database)
    with dispatch.FilesInZipDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Zip,
        lookups=lookups,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))
    assert isfile(target_name)


@pytest.mark.parametrize(
    'temporal_key,naming_keys',
    [("decade", ("gender_id", "party_id")), ("year", ("gender_id",)), ("decade", [])],
)
def test_organized_speeches_in_zip_dispatch(
    tagged_speeches: list[dict[str, sg.DispatchItem]], temporal_key: str, naming_keys: list[str]
):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}.zip'
    lookups: md.Codecs = md.Codecs().load(database)
    with dispatch.SortedSpeechesInZipDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Zip,
        subfolder_key=temporal_key,
        naming_keys=naming_keys,
        lookups=lookups,
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


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_checkpoint_dispatch(
    tagged_speeches: list[dict[str, sg.DispatchItem]], source_index: corpus_index.CorpusSourceIndex
):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}'
    lookups: md.Codecs = md.Codecs().load(database)
    with dispatch.CheckpointPerGroupDispatcher(
        target_name=target_name,
        compress_type=dispatch.CompressType.Zip,
        lookups=lookups,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert isdir(target_name)

    files_on_disk: Set[str] = set(basename(x) for x in glob.glob(join(target_name, '**/prot-*.zip'), recursive=True))
    files_in_index: Set[str] = set(source_index.filenames)
    assert (files_in_index - files_on_disk) == set()


def files_in_folder(folder: str, *, pattern: str, strip_path: bool = True, strip_ext: bool = True) -> Set[str]:
    files: set[str] = set(basename(x) for x in glob.glob(join(folder, pattern), recursive=True))
    if strip_path:
        files = {basename(x) for x in files}
    if strip_ext:
        files = {utility.strip_extensions(x) for x in files}
    return files


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
@pytest.mark.parametrize('cls', [dispatch.TaggedFramePerGroupDispatcher, dispatch.IdTaggedFramePerGroupDispatcher])
def test_single_feather_per_group_dispatch(
    tagged_speeches: list[dict[str, sg.DispatchItem]],
    source_index: corpus_index.CorpusSourceIndex,
    cls: Type[dispatch.IDispatcher],
):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}'
    lookups: md.Codecs = md.Codecs().load(database)
    with cls(
        target_name=target_name,
        compress_type=dispatch.CompressType.Feather,
        lookups=lookups,
    ) as dispatcher:
        for group in tagged_speeches:
            dispatcher.dispatch(list(group.values()))

    assert isdir(target_name)

    files_on_disk: Set[str] = files_in_folder(target_name, pattern='**/prot-*.feather')
    files_in_index: Set[str] = set(utility.strip_extensions(source_index.filenames))

    assert (files_in_index - files_on_disk) == set()
    assert isfile(join(target_name, 'document_index.feather'))


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
@pytest.mark.parametrize('cls', [dispatch.TaggedFramePerGroupDispatcher, dispatch.IdTaggedFramePerGroupDispatcher])
def test_single_feather_per_group_dispatch_with_skips(
    tagged_speeches: list[dict[str, sg.DispatchItem]],
    source_index: corpus_index.CorpusSourceIndex,
    cls: Type[dispatch.IDispatcher],
):
    database: str = ConfigStore.config().get("metadata.database.options.filename")
    target_name: str = f'./tests/output/{str(uuid.uuid1())[:8]}'
    lookups: md.Codecs = md.Codecs().load(database)
    with cls(
        target_name=target_name,
        compress_type=dispatch.CompressType.Feather,
        lookups=lookups,
        lowercase=True,
        skip_text=True,
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

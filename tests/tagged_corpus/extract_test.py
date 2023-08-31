import glob
import os
import shutil
import uuid
from os.path import basename, isdir, isfile, join
from typing import Iterable

import pandas as pd
import pytest

from pyriksprot import CorpusSourceIndex, interface, to_speech, workflows
from pyriksprot.corpus import tagged as tagged_corpus
from pyriksprot.dispatch import dispatch
from pyriksprot.utility import touch

from ..utility import SAMPLE_METADATA_DATABASE_NAME, TAGGED_SOURCE_FOLDER, sample_tagged_frames_corpus_exists

# pylint: disable=redefined-outer-name,no-member


def remove_intermediate_subfolders(source_folder):
    """Removes folders in source folder having an extra level of subfolders with the same name."""
    folders: list[str] = [folder for folder in glob.glob(join(source_folder, "*"), recursive=False) if isdir(folder)]
    for folder in folders:
        subfolder = join(folder, basename(folder))
        if isdir(subfolder):
            for filename in glob.glob(join(subfolder, "*"), recursive=False):
                shutil.move(filename, folder)
            os.rmdir(subfolder)


def test_remove_intermediate_subfolders():
    # source_folder: str = "/data/riksdagen_corpus_data/v0.9.0/tagged_frames"
    source_folder: str = f"./tests/output/{str(uuid.uuid4())[:8]}"
    shutil.rmtree(source_folder, ignore_errors=True)

    os.makedirs(source_folder, exist_ok=True)

    os.makedirs(f"{source_folder}/A/A", exist_ok=True)

    touch(f"{source_folder}/x-1")

    touch(f"{source_folder}/A/A/a-1")
    touch(f"{source_folder}/A/A/a-2")
    touch(f"{source_folder}/A/a-3")

    os.makedirs(f"{source_folder}/B/B", exist_ok=True)
    touch(f"{source_folder}/B/B/b-1")

    os.makedirs(f"{source_folder}/C/D", exist_ok=True)
    touch(f"{source_folder}/C/D/d-1")

    remove_intermediate_subfolders(source_folder)

    assert isfile(f"{source_folder}/x-1")

    assert isdir(f"{source_folder}/A")
    assert isfile(f"{source_folder}/A/a-1")
    assert isfile(f"{source_folder}/A/a-2")
    assert isfile(f"{source_folder}/A/a-3")
    assert not isdir(f"{source_folder}/A/A")

    assert isdir(f"{source_folder}/B")
    assert isfile(f"{source_folder}/B/b-1")
    assert not isdir(f"{source_folder}/B/B")

    assert isdir(f"{source_folder}/C/D")
    assert isfile(f"{source_folder}/C/D/d-1")

    shutil.rmtree(source_folder, ignore_errors=True)


def test_fix_extra_subfolders():
    source_folder: str = "/data/riksdagen_corpus_data/v0.9.0/tagged_frames"
    remove_intermediate_subfolders(source_folder)


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_glob_protocols():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    filenames: list[str] = tagged_corpus.glob_protocols(corpus_source, pattern='**/prot-*.zip', strip_path=True)
    assert len(filenames) == 6
    """Empty files should be included"""
    assert 'prot-1955--ak--22.zip' in filenames


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_create_source_index_for_tagged_corpus():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    source_index = CorpusSourceIndex.load(source_folder=corpus_source, source_pattern='**/prot-*.zip')
    assert isinstance(source_index, CorpusSourceIndex)
    assert len(source_index) == 5

    source_index = CorpusSourceIndex.load(source_folder=corpus_source, source_pattern='**/prot-*.zip', skip_empty=False)
    assert len(source_index) == 6


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_load_protocols():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    filenames: list[str] = tagged_corpus.glob_protocols(corpus_source, pattern='**/prot-*.zip')

    protocol_iter: Iterable[interface.Protocol] = tagged_corpus.load_protocols(corpus_source)
    protocols = list(protocol_iter)

    """Empty files should NOT be included"""
    assert len(protocols) == len(filenames) - 1


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
@pytest.mark.parametrize(
    'temporal_key, group_keys',
    [
        (interface.TemporalKey.Year, ["party_id"]),
        (interface.TemporalKey.Year, []),
        (interface.TemporalKey.Protocol, []),
        (None, []),
    ],
)
def test_extract_corpus_tags_with_various_groupings(temporal_key, group_keys):
    target_name = f'tests/output/{temporal_key}_{"_".join(group_keys)}_{uuid.uuid1()}.zip'

    opts = {
        **dict(
            source_folder=TAGGED_SOURCE_FOLDER,
            metadata_filename=SAMPLE_METADATA_DATABASE_NAME,
            target_type='files-in-zip',
            content_type=interface.ContentType.TaggedFrame,
            segment_level=interface.SegmentLevel.Speech,
            multiproc_keep_order=None,
            multiproc_processes=1,
            years=None,
            segment_skip_size=1,
        ),
        **dict(
            target_name=target_name,
            temporal_key=temporal_key,
            group_keys=group_keys,
        ),
    }

    workflows.extract_corpus_tags(**opts, progress=False)
    assert isfile(opts['target_name'])
    os.unlink(opts['target_name'])


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
@pytest.mark.parametrize(
    'target_type,merge_strategy,compress_type',
    [
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.who_speaker_note_id_sequence, 'csv'),
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.who_speaker_note_id_sequence, 'feather'),
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.chain, 'csv'),
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.chain, 'feather'),
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.speaker_note_id_sequence, 'csv'),
        ('single-id-tagged-frame-per-group', to_speech.MergeStrategyType.speaker_note_id_sequence, 'feather'),
    ],
)
def test_extract_speeches(target_type: str, merge_strategy: to_speech.MergeStrategyType, compress_type: str):
    target_name: str = f'tests/output/speech_{str(uuid.uuid1())[:6]}_{merge_strategy}'

    fixed_opts: dict = dict(
        source_folder=TAGGED_SOURCE_FOLDER,
        metadata_filename=SAMPLE_METADATA_DATABASE_NAME,
        segment_level=interface.SegmentLevel.Speech,
        temporal_key=interface.TemporalKey.NONE,
        content_type=interface.ContentType.TaggedFrame,
        multiproc_keep_order=None,
        multiproc_processes=None,
        multiproc_chunksize=100,
        segment_skip_size=1,
        years=None,
        group_keys=None,
        force=True,
        skip_lemma=False,
        skip_text=True,
        skip_puncts=True,
        skip_stopwords=True,
        lowercase=True,
        progress=False,
    )
    workflows.extract_corpus_tags(
        **fixed_opts,
        target_name=target_name,
        target_type=target_type,
        compress_type=dispatch.CompressType(compress_type),
        merge_strategy=merge_strategy,
    )

    assert isdir(target_name)
    assert isfile(join(target_name, f'document_index.{compress_type}'))
    assert isfile(join(target_name, f'token2id.{compress_type}'))
    # assert isfile(join(target_name, 'person_index.zip'))

    target_filename: str = join(target_name, f'document_index.{compress_type}')
    document_index: pd.DataFrame = (
        pd.read_csv(target_filename, sep='\t')
        if compress_type == 'csv'
        else pd.read_feather(target_filename)
        if compress_type == 'feather'
        else None
    )
    assert 'party_id' in document_index.columns
    assert 'gender_id' in document_index.columns

import os

import pytest
from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.utility import download_protocols

from .utility import (
    RIKSPROT_PARLACLARIN_FOLDER,
    RIKSPROT_REPOSITORY_TAG,
    TAGGED_SOURCE_FOLDER,
    TEST_DOCUMENTS,
    create_subset_metadata_to_folder,
    ensure_test_corpora_exist,
    sample_tagged_corpus_exists,
    sample_xml_corpus_exists,
    setup_sample_speech_corpora,
    setup_sample_tagged_frames_corpus,
)

jj = os.path.join

FORCE_RUN_SKIPS = os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(condition=sample_xml_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus():

    protocols: list[str] = TEST_DOCUMENTS
    target_folder: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols")
    download_protocols(
        protocols=protocols, target_folder=target_folder, create_subfolder=True, tag=RIKSPROT_REPOSITORY_TAG
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_to_folder():
    target_folder: str = './metadata/data/'
    md.download_to_folder(tag=RIKSPROT_REPOSITORY_TAG, folder=target_folder, force=True)
    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in md.RIKSPROT_METADATA_TABLES)


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_corpus_exists(), reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus():
    setup_sample_tagged_frames_corpus(
        protocols=TEST_DOCUMENTS,
        source_folder=os.environ["TEST_RIKSPROT_TAGGED_FOLDER"],
        target_folder=TAGGED_SOURCE_FOLDER,
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_create_subset_metadata_to_folder():
    logger.info("creating sub-setted test metadata")
    create_subset_metadata_to_folder()


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist()


def test_setup_sample_speech_corpora():
    setup_sample_speech_corpora()

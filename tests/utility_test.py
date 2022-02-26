import os
from pathlib import Path

import pytest

from pyriksprot import metadata
from pyriksprot.utility import dedent, download_protocols, temporary_file

from .utility import (
    PARLACLARIN_SOURCE_FOLDER,
    PARLACLARIN_SOURCE_TAG,
    TAGGED_SOURCE_FOLDER,
    TEST_DOCUMENTS,
    sample_metadata_exists,
    sample_tagged_corpus_exists,
    sample_xml_corpus_exists,
    setup_sample_tagged_frames_corpus,
)

jj = os.path.join


def test_temporary_file():

    filename = jj("tests", "output", "trazan.txt")

    with temporary_file(filename=filename) as path:
        path.touch()
        assert path.is_file(), "file doesn't exists"
    assert not Path(filename).is_file(), "file exists"

    with temporary_file(filename=filename, content="X") as path:
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"

    with temporary_file(filename=None, content="X") as path:
        filename = str(path)
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"


def test_dedent():
    assert dedent("") == ""
    assert dedent("apa\napa") == "apa\napa"
    assert dedent("apa\n\napa\n") == "apa\n\napa\n"
    assert dedent("apa\n\n  apa\n") == "apa\n\napa\n"
    assert dedent("\tapa\n\n  \tapa \t\n") == "apa\n\napa\n"


# @pytest.mark.skipif(condition=sample_metadata_exists(), reason="Test data found")
def test_setup_sample_metadata():

    target_folder: str = jj(PARLACLARIN_SOURCE_FOLDER, "metadata")
    metadata.download_to_folder(tag=PARLACLARIN_SOURCE_TAG, folder=target_folder)


@pytest.mark.skipif(condition=sample_xml_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus():

    protocols: list[str] = TEST_DOCUMENTS
    target_folder: str = jj(PARLACLARIN_SOURCE_FOLDER, "protocols")
    download_protocols(
        protocols=protocols, target_folder=target_folder, create_subfolder=True, tag=PARLACLARIN_SOURCE_TAG
    )


@pytest.mark.skipif(condition=sample_tagged_corpus_exists(), reason="Test data found")
def test_setup_sample_tagged_frames_corpus():
    setup_sample_tagged_frames_corpus(
        protocols=TEST_DOCUMENTS,
        source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"],
        target_folder=TAGGED_SOURCE_FOLDER,
    )

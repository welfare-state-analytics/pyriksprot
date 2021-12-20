import os
from os.path import join as jj
from pathlib import Path

import pytest

from pyriksprot.utility import dedent, temporary_file

from .utility import (
    PARLACLARIN_SOURCE_FOLDER,
    TAGGED_SOURCE_FOLDER,
    TEST_DOCUMENTS,
    setup_parlaclarin_test_corpus,
    setup_tagged_frames_test_corpus,
)


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


@pytest.mark.skipif(condition=os.path.isdir(PARLACLARIN_SOURCE_FOLDER), reason="Test data found")
def test_setup_parlaclarin_test_corpus():
    setup_parlaclarin_test_corpus(
        protocols=TEST_DOCUMENTS,
        target_folder=PARLACLARIN_SOURCE_FOLDER,
    )


@pytest.mark.skipif(condition=os.path.isdir(TAGGED_SOURCE_FOLDER), reason="Test data found")
def test_setup_tagged_frames_corpus():
    setup_tagged_frames_test_corpus(
        protocols=TEST_DOCUMENTS,
        source_folder=os.environ["PARLACLARIN_TAGGED_FOLDER"],
        target_folder=TAGGED_SOURCE_FOLDER,
    )

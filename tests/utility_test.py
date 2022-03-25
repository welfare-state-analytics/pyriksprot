import os
from pathlib import Path

import pytest

from pyriksprot.utility import dedent, probe_filename, replace_extension, temporary_file, touch

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


def test_probe_filename():

    filename: str = "./tests/output/apa.csv"
    touch(replace_extension(filename, "txt"))

    with pytest.raises(FileNotFoundError):
        probe_filename(filename)

    assert probe_filename(filename, exts=["txt"]) is not None
    assert probe_filename(filename, exts=[".txt"]) is not None

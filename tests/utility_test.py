import os
from pathlib import Path

import pytest

from pyriksprot import utility as pu

jj = os.path.join


def test_temporary_file():

    filename = jj("tests", "output", "trazan.txt")

    with pu.temporary_file(filename=filename) as path:
        path.touch()
        assert path.is_file(), "file doesn't exists"
    assert not Path(filename).is_file(), "file exists"

    with pu.temporary_file(filename=filename, content="X") as path:
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"

    with pu.temporary_file(filename=None, content="X") as path:
        filename = str(path)
        assert path.is_file(), "file doesn't exists"
        with open(filename, "r", encoding="utf-8") as fp:
            assert fp.read() == "X"
    assert not Path(filename).is_file(), "file exists"


def test_dedent():
    assert pu.dedent("") == ""
    assert pu.dedent("apa\napa") == "apa\napa"
    assert pu.dedent("apa\n\napa\n") == "apa\n\napa\n"
    assert pu.dedent("apa\n\n  apa\n") == "apa\n\napa\n"
    assert pu.dedent("\tapa\n\n  \tapa \t\n") == "apa\n\napa\n"


def test_probe_filename():

    filename: str = "./tests/output/apa.csv"
    pu.touch(pu.replace_extension(filename, "txt"))

    with pytest.raises(FileNotFoundError):
        pu.probe_filename(filename)

    assert pu.probe_filename(filename, exts=["txt"]) is not None
    assert pu.probe_filename(filename, exts=[".txt"]) is not None


def test_repository_tags():
    tags = pu.repository_tags()
    assert len(tags) > 0

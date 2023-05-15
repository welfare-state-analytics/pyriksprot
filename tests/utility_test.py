import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pytest

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import preprocess as pr
from pyriksprot import utility as pu
from pyriksprot.corpus.iterate import ProtocolSegment
from pyriksprot.utility import dotexpand
from tests.utility import RIKSPROT_PARLACLARIN_FAKE_FOLDER

from . import fakes

jj = os.path.join


@pytest.mark.parametrize(
    'document_name,',
    [
        'prot-1958-fake',
        'prot-1960-fake',
        'prot-1980-empty',
    ],
)
def test_load_fakes(document_name: str):
    filename: str = jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, f"{document_name}.xml")

    utterances: list[interface.Utterance] = fakes.load_sample_utterances(filename)

    tags: pd.DataFrame = fakes.load_sample_tagged_dataframe(filename)

    assert len(utterances) == 0 or len(tags) > 0

    u_segments: Iterable[ProtocolSegment] = fakes.load_segment_stream(filename, interface.SegmentLevel.Utterance)
    assert len(list(u_segments)) == len(utterances)

    p_segment: Iterable[ProtocolSegment] = fakes.load_segment_stream(filename, interface.SegmentLevel.Protocol)
    assert len(utterances) == 0 or len(list(p_segment)) == 1

    who_segments: Iterable[ProtocolSegment] = fakes.load_segment_stream(filename, interface.SegmentLevel.Who)
    assert len(utterances) == 0 or len(list(who_segments)) > 0

    speeches: Iterable[ProtocolSegment] = fakes.load_segment_stream(filename, interface.SegmentLevel.Speech)
    assert len(utterances) == 0 or len(list(speeches)) > 0


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


def test_dotexpand():
    assert not dotexpand("")
    assert dotexpand("a") == ["a"]
    assert dotexpand("a.b") == ["a.b"]
    assert dotexpand("a.b,c.d") == ["a.b", "c.d"]
    assert dotexpand("a:b,c.d") == ["a.b", "a_b", "c.d"]
    assert dotexpand("a:b, c.d") == ["a.b", "a_b", "c.d"]


def test_dotget():
    assert pu.dget({}, "apa", None) is None
    assert pu.dget({}, "apa", default="olle") == "olle"
    assert pu.dget({}, "apa.olle", default="olle") == "olle"
    assert pu.dget({}, "apa:olle", default="olle") == "olle"
    assert pu.dget({'olle': 99}, "olle") == 99
    assert pu.dget({'olle': 99}, "olle.olle") is None
    assert pu.dget({'olle': {'kalle': 99, 'erik': 98}}, "olle.olle") is None
    assert pu.dget({'olle': {'kalle': 99, 'erik': 98}}, "olle.erik") == 98
    assert pu.dget({'olle': {'kalle': 99, 'erik': 98}}, "olle.kalle") == 99
    assert pu.dget({'olle': {'kalle': 99, 'erik': 98}}, "erik", "olle.kalle") == 99
    assert pu.dget({'olle': {'kalle': 99, 'erik': 98}}, "olle:kalle") == 99
    assert pu.dget({'olle_kalle': 99, 'erik': 98}, "olle:kalle") == 99

    assert pu.dotget({}, "apa", default=None) is None
    assert pu.dotget({}, "apa", default="olle") == "olle"
    assert pu.dotget({}, "apa.olle", default="olle") == "olle"
    assert pu.dotget({'olle': 99}, "olle") == 99
    assert pu.dotget({'olle': 99}, "olle.olle") is None
    assert pu.dotget({'olle': {'kalle': 99, 'erik': 98}}, "olle.olle") is None
    assert pu.dotget({'olle': {'kalle': 99, 'erik': 98}}, "olle.erik") == 98
    assert pu.dotget({'olle': {'kalle': 99, 'erik': 98}}, "olle.kalle") == 99
    assert pu.dotget({'olle': {'kalle': 99, 'erik': 98}}, "olle:kalle") == 99


def test_dedent():
    assert pr.dedent("") == ""
    assert pr.dedent("apa\napa") == "apa\napa"
    assert pr.dedent("apa\n\napa\n") == "apa\n\napa\n"
    assert pr.dedent("apa\n\n  apa\n") == "apa\n\napa\n"
    assert pr.dedent("\tapa\n\n  \tapa \t\n") == "apa\n\napa\n"


def test_probe_filename():
    filename: str = "./tests/output/apa.csv"
    pu.touch(pu.replace_extension(filename, "txt"))

    with pytest.raises(FileNotFoundError):
        pu.probe_filename(filename)

    assert pu.probe_filename(filename, exts=["txt"]) is not None
    assert pu.probe_filename(filename, exts=[".txt"]) is not None


def test_repository_tags():
    tags = md.gh_tags()
    assert len(tags) > 0


SAMPLE_DATISH_VALUES: list[str | None] = ['2020', None, '2023-02', '2023-03-24']


def _sample_datish_dataframe() -> list[str | None]:
    return pd.DataFrame(data={'dt': SAMPLE_DATISH_VALUES})


def test_fix_incomplete_datetime_series_truncate_inplace():
    df: pd.DataFrame = _sample_datish_dataframe()

    md.fix_incomplete_datetime_series(df, "dt", action="truncate", inplace=True)

    assert 'dt0' in df.columns
    assert 'dt_flag' in df.columns

    assert df.dt0.equals(pd.Series(SAMPLE_DATISH_VALUES))
    assert df.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))

    expected_values = pd.Series([pd.Timestamp(2020, 1, 1), pd.NaT, pd.Timestamp(2023, 2, 1), pd.Timestamp(2023, 3, 24)])

    assert pd.to_datetime(df.dt).equals(expected_values)


def test_fix_incomplete_datetime_series_extend_inplace():
    df: pd.DataFrame = _sample_datish_dataframe()
    md.fix_incomplete_datetime_series(df, "dt", action="extend", inplace=True)

    assert df.dt0.equals(pd.Series(SAMPLE_DATISH_VALUES))
    assert df.dt.equals(pd.Series(['2020-12-31', np.nan, '2023-02-28', '2023-03-24']))
    assert df.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))


def test_fix_incomplete_datetime_series_extend_not_inplace():
    df: pd.DataFrame = _sample_datish_dataframe()
    df2 = md.fix_incomplete_datetime_series(df, "dt", action="extend", inplace=False)

    assert 'df0' not in df.columns
    assert 'df_flag' not in df.columns

    assert df2.dt0.equals(pd.Series(SAMPLE_DATISH_VALUES))
    assert df2.dt.equals(pd.Series(["2020-12-31", np.NaN, "2023-02-28", '2023-03-24']))
    assert df2.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))

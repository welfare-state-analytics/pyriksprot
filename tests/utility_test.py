import os
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pyriksprot import metadata as md
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
    tags = md.gh_tags()
    assert len(tags) > 0


def test_complete_datetime_series():

    df: pd.DataFrame = pd.DataFrame(data={'dt': ['2020', None, '2023-02', '2023-03-24']})

    md.fix_incomplete_datetime_series(df, "dt", action="truncate", inplace=True)

    assert 'dt0' in df.columns
    assert 'dt_flag' in df.columns
    df['dt'] = pd.to_datetime(df.dt)
    assert df.dt0.equals(pd.Series(['2020', None, '2023-02', '2023-03-24']))
    assert df.dt.fillna(0).equals(pd.Series([date(2020, 1, 1), 0, date(2023, 2, 1), date(2023, 3, 24)]))
    assert df.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))

    df: pd.DataFrame = pd.DataFrame(data={'dt': ['2020', None, '2023-02', '2023-03-24']})
    md.fix_incomplete_datetime_series(df, "dt", action="extend", inplace=True)

    assert df.dt0.equals(pd.Series(['2020', None, '2023-02', '2023-03-24']))
    assert df.dt.equals(pd.Series(['2020-12-31', np.nan, '2023-02-28', '2023-03-24']))
    assert df.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))

    df: pd.DataFrame = pd.DataFrame(data={'dt': ['2020', None, '2023-02', '2023-03-24']})
    df2 = md.fix_incomplete_datetime_series(df, "dt", action="extend", inplace=False)

    assert 'df0' not in df.columns
    assert 'df_flag' not in df.columns

    assert df2.dt0.equals(pd.Series(['2020', None, '2023-02', '2023-03-24']))
    assert df2.dt.equals(pd.Series(["2020-12-31", np.NaN, "2023-02-28", '2023-03-24']))
    assert df2.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))

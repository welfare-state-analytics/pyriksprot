import os
from pathlib import Path
from typing import Iterable
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from pyriksprot import gitchen as gh
from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import preprocess as pr
from pyriksprot import utility as pu
from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.corpus.iterate import ProtocolSegment
from pyriksprot.corpus.utility import get_chamber_by_filename, ls_corpus_by_tei_corpora, ls_corpus_folder

from . import fakes

jj = os.path.join


def test_format_protocol_id():

    assert pu.format_protocol_id("prot-199495--011.xml") == "1994/95:11"
    assert pu.format_protocol_id("prot-199495--011") == "1994/95:11"
    assert pu.format_protocol_id("prot-199495--11") == "1994/95:11"

    assert pu.format_protocol_id("prot-1945--ak--011.xml") == "Andra kammaren 1945:11"
    assert pu.format_protocol_id("prot-1945--ak--011") == "Andra kammaren 1945:11"

    assert pu.format_protocol_id("prot-1945--fk--016.xml") == "Första kammaren 1945:16"
    assert pu.format_protocol_id("prot-1945--fk--016") == "Första kammaren 1945:16"

    assert pu.format_protocol_id("prot-1945--fk--016_099.xml") == "Första kammaren 1945:16 099"
    assert pu.format_protocol_id("prot-1945--fk--016_99") == "Första kammaren 1945:16 99"

    assert pu.format_protocol_id("prot-19992000--089") == "1999/2000:89"


def test_ls_corpus_by_tei_corpora():
    folder: str = ConfigStore.config().get("corpus.folder")

    dict_data: dict[str, dict[str, str | list[str]]] = ls_corpus_by_tei_corpora(
        folder=folder, mode='dict', normalize=False
    )

    assert isinstance(dict_data, dict)
    assert len(dict_data) == 3
    assert set(dict_data.keys()) == {'ak', 'ek', 'fk'}
    assert len(dict_data['ak']['filenames']) == 1
    assert len(dict_data['fk']['filenames']) == 1
    assert len(dict_data['ek']['filenames']) == 4

    filenames: list[str] = ls_corpus_by_tei_corpora(folder=folder, mode='filenames', normalize=False)
    assert isinstance(filenames, list)
    assert len(filenames) == 6
    assert './199192/prot-199192--127.xml' in filenames

    filenames = ls_corpus_by_tei_corpora(folder=folder, mode='filenames', normalize=True)
    assert isinstance(filenames, list)
    assert len(filenames) == 6
    assert all(f.startswith('/') for f in filenames)
    assert any(f.endswith('199192/prot-199192--127.xml') for f in filenames)

    tuples: list[str] = ls_corpus_by_tei_corpora(folder=folder, mode='tuples', normalize=False)
    assert isinstance(tuples, list)
    assert len(tuples) == 6
    assert ('ek', './199192/prot-199192--127.xml') in tuples

    filenames = ls_corpus_folder(folder=folder)
    assert isinstance(filenames, list)
    assert len(filenames) == 6
    assert all(f.startswith(folder) for f in filenames)
    assert any(f.endswith('199192/prot-199192--127.xml') for f in filenames)


@pytest.mark.parametrize(
    'document_name,',
    [
        'prot-1958-fake',
        'prot-1960-fake',
        'prot-1980-empty',
    ],
)
def test_load_fakes(document_name: str):
    fakes_folder: str = ConfigStore.config().get("fakes.folder")
    filename: str = jj(fakes_folder, f"{document_name}.xml")

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
    assert not pu.dotexpand("")
    assert pu.dotexpand("a") == ["a"]
    assert pu.dotexpand("a.b") == ["a.b"]
    assert pu.dotexpand("a.b,c.d") == ["a.b", "c.d"]
    assert pu.dotexpand("a:b,c.d") == ["a.b", "a_b", "c.d"]
    assert pu.dotexpand("a:b, c.d") == ["a.b", "a_b", "c.d"]


def test_dotset():
    d = {}
    pu.dotset(d, "a.b", 1)
    assert d == {"a": {"b": 1}}


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


@pytest.mark.parametrize(
    'prefix, mock_env, expected',
    [
        (
            'TEST_PREFIX',
            {'TEST_PREFIX_KEY1': 'value1', 'TEST_PREFIX_KEY2': 'value2'},
            {'key1': 'value1', 'key2': 'value2'},
        ),
        (
            'ABC',
            {'ABC:KEY1': 'value1', 'ABC:KEY2': 'value2'},
            {'key1': 'value1', 'key2': 'value2'},
        ),
        (
            'ABC',
            {'ABC.KEY1': 'value1', 'ABC.KEY2': 'value2'},
            {'key1': 'value1', 'key2': 'value2'},
        ),
        (
            '',
            {'TEST_PREFIX_KEY1': 'value1', 'TEST_PREFIX_KEY2': 'value2'},
            {},
        ),
        (
            'TEST_PREFIX',
            {'OTHER_KEY1': 'value1', 'OTHER_KEY2': 'value2'},
            {},
        ),
    ],
)
def test_env2dict(prefix: str, mock_env: dict[str, str], expected: dict[str, str]):
    with patch.dict(os.environ, mock_env):
        result: dict[str, str] = pu.env2dict(prefix=prefix)
        assert result == expected


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
    folder: str = ConfigStore.config().get("corpus.folder")
    tags = gh.gh_tags(folder)
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
    assert df2.dt.equals(pd.Series(["2020-12-31", np.nan, "2023-02-28", '2023-03-24']))
    assert df2.dt_flag.equals(pd.Series(['Y', 'X', 'M', 'D']))


def test_get_chamber_by_filename():
    assert get_chamber_by_filename("prot-199495--011.xml") == "ek"
    assert get_chamber_by_filename("prot-199495--011") == "ek"
    assert get_chamber_by_filename("prot-199495--11") == "ek"
    assert get_chamber_by_filename("prot-1945--ak--011.xml") == "ak"
    assert get_chamber_by_filename("prot-1945--ak--011") == "ak"
    assert get_chamber_by_filename("prot-1945--fk--016.xml") == "fk"
    assert get_chamber_by_filename("prot-1945--fk--016") == "fk"
    assert get_chamber_by_filename("prot-1945--fk--016_099.xml") == "fk"
    assert get_chamber_by_filename("prot-1945--fk--016_99") == "fk"
    assert get_chamber_by_filename("prot-19992000--089") == "ek"

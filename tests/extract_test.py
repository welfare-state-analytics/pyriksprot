import base64
import glob
import os
import sys
import uuid
import zlib
from typing import Iterable, List

import pytest

import pyriksprot
from pyriksprot import iterators, model, persist
from pyriksprot.extract_text import (
    AggregateIterItem,
    ParliamentaryMemberIndex,
    SourceIndex,
    TextAggregator,
    create_grouping_hashcoder,
)
from pyriksprot.interface import ProtocolIterItem

# pylint: disable=redefined-outer-name


TEST_CORPUS_FOLDER = '/data/riksdagen_corpus_data/riksdagen-corpus/corpus'
# TEST_CORPUS_FOLDER ='tests/test_data/source'


@pytest.fixture
def member_index() -> ParliamentaryMemberIndex:
    return ParliamentaryMemberIndex(f'{TEST_CORPUS_FOLDER}/members_of_parliament.csv')


@pytest.fixture
def source_index() -> SourceIndex:

    """Load from folder"""
    # source_folder = '/data/riksdagen_corpus_data/riksdagen-corpus/corpus'
    # source_folder = 'tests/test_data/xml'
    # source_index: SourceIndex = SourceIndex.load(source_folder)
    # source_index.to_csv('tests/test_data/source_index.csv')

    """Read from file"""
    # source_index = SourceIndex.read_csv('tests/test_data/source_index.csv')

    """Test fixture"""
    source_index: SourceIndex = SourceIndex.load(TEST_CORPUS_FOLDER)
    return source_index


def test_create_grouping_hashcoder(source_index: SourceIndex, member_index: ParliamentaryMemberIndex):

    attributes = ['who', 'gender']
    hashcoder = create_grouping_hashcoder(attributes)

    assert callable(hashcoder)

    item: ProtocolIterItem = ProtocolIterItem(
        id="a",
        name="apa",
        page_number="0",
        text="hej",
        who="alexis_bjorkman_7f7c23",
    )
    hashcode = hashcoder(item, member_index['alexis_bjorkman_7f7c23'], source_index)

    assert hashcode is not None


def test_parliamentary_index():

    member_index = ParliamentaryMemberIndex()

    assert isinstance(member_index.members, dict)
    assert len(member_index.members) > 0


# @pytest.mark.parametrize(
#     'iterator_class',
#     [
#         iterators.ProtocolTextIterator,
#         iterators.XmlProtocolTextIterator,
#         iterators.XmlIterProtocolTextIterator,
#     ],
# )


def test_aggregator_aggregate(source_index, member_index):

    # filenames: List[str] = glob.glob('tests/test_data/source/**/prot-*.xml', recursive=True)
    filenames: List[str] = glob.glob(f'{TEST_CORPUS_FOLDER}/**/prot-*.xml', recursive=True)

    texts: Iterable[ProtocolIterItem] = iterators.XmlProtocolTextIterator(
        filenames=filenames, level='speaker', skip_size=0, processes=None
    )

    aggregator: TextAggregator = TextAggregator(
        source_index=source_index,
        member_index=member_index,
        temporal_key='year',
        grouping_keys=['party'],
    )

    assert aggregator is not None

    data: List[AggregateIterItem] = []
    for item in aggregator.aggregate(texts):
        print(item)
        data.append(item)

    assert len(data) > 0


def test_extract_corpus_text():

    default_opts = {
        'source_folder': 'tests/test_data/source',
        'target': 'tests/output/',
        'level': 'speaker',
        'dedent': False,
        'dehyphen': False,
        'keep_order': False,
        'skip_size': 1,
        'processes': None,
        'years': None,
        'temporal_key': None,
        'group_keys': None,
        '_': {},
    }

    opts = {**default_opts, **dict(temporal_key='year', group_keys=['party'])}

    opts = {
        'source_folder': '/data/riksdagen_corpus_data/riksdagen-corpus/corpus',
        'target': '.',
        'level': 'speaker',
        'dedent': False,
        'dehyphen': False,
        'keep_order': False,
        'skip_size': 1,
        'processes': None,
        'years': '1920',
        'temporal_key': 'year',
        'group_keys': ('party', 'gender', 'who'),
        '_': {},
    }

    pyriksprot.extract_corpus_text(**opts)

    opts = {**default_opts, **dict(years='1920')}
    pyriksprot.extract_corpus_text(**opts)


def test_aggregator_extract_gender_party_no_time_period():

    target_filename: str = f'tests/output/{uuid.uuid1()}.zip'
    opts = {
        'source_folder': 'tests/test_data/source',
        'target': target_filename,
        'target_mode': 'zip',
        'level': 'who',
        'dedent': False,
        'dehyphen': False,
        'keep_order': False,
        'skip_size': 1,
        'processes': None,
        'years': '1955-1965',
        'temporal_key': 'protocol',
        'group_keys': ('party', 'gender'),
        '_': {},
    }

    pyriksprot.extract_corpus_text(**opts)

    assert os.path.isfile(target_filename)

    os.unlink(target_filename)


import glob
from typing import Iterable, List

import pytest

import pyriksprot
from pyriksprot import iterators
from pyriksprot.extract import (
    AggregateIterItem,
    ParliamentaryMemberIndex,
    SourceIndex,
    TextAggregator,
    create_grouping_hashcoder,
)
from pyriksprot.interface import ProtocolIterItem

# pylint: disable=redefined-outer-name


@pytest.fixture
def member_index() -> ParliamentaryMemberIndex:
    return ParliamentaryMemberIndex('tests/test_data/source/members_of_parliament.csv')


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
    source_index: SourceIndex = SourceIndex.load('tests/test_data/source')
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


def test_parliamentary_index(member_index):

    # member_index = ParliamentaryMemberIndex()

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

    filenames: List[str] = glob.glob('tests/test_data/source/**/prot-*.xml', recursive=True)

    texts: Iterable[ProtocolIterItem] = iterators.XmlProtocolTextIterator(
        filenames=filenames, level='speaker', skip_size=0, processes=None
    )

    aggregator: TextAggregator = TextAggregator(
        source_index=source_index,
        member_index=member_index,
        temporal_key='year',
        grouping_keys=['who'],
    )

    assert aggregator is not None

    data: List[AggregateIterItem] = []
    for item in aggregator.aggregate(texts):
        data.append(item)

    assert len(data) > 0


@pytest.mark.skip(reason="WIP")
def test_extract_corpus_text():

    pyriksprot.extract_corpus_text(
        source_folder='/data/riksdagen_corpus_data/riksdagen-corpus/corpus',
        source_pattern='**/*.xml',
        years='1939,1940-1942',
        target="apa/",
        level='speaker',
        dedent=False,
        dehyphen=False,
        processes=1,
        groupby=None,
    )

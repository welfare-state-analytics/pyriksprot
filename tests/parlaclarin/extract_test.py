import glob
import os
import uuid
from typing import Iterable, List, Mapping

import pytest

from pyriksprot import corpus_index, interface, member, merge, parlaclarin

# pylint: disable=redefined-outer-name


# TEST_CORPUS_FOLDER = '/data/riksdagen_corpus_data/riksdagen-corpus/corpus'
TEST_CORPUS_FOLDER = 'tests/test_data/source'


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(f'{TEST_CORPUS_FOLDER}/members_of_parliament.csv')


@pytest.fixture
def source_index() -> corpus_index.CorpusSourceIndex:

    """Load from folder"""
    # source_folder = '/data/riksdagen_corpus_data/riksdagen-corpus/corpus'
    # source_folder = 'tests/test_data/xml'
    # source_index: CorpusSourceIndex = CorpusSourceIndex.load(source_folder)
    # source_index.to_csv('tests/test_data/source_index.csv')

    """Read from file"""
    # source_index = CorpusSourceIndex.read_csv('tests/test_data/source_index.csv')

    """Test fixture"""
    source_index: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=TEST_CORPUS_FOLDER,
        source_pattern='**/prot-*.xml',
        skip_empty=False,
    )
    return source_index


def test_create_grouping_hashcoder(
    source_index: corpus_index.CorpusSourceIndex, member_index: member.ParliamentaryMemberIndex
):

    attributes = [interface.SegmentLevel.Who, interface.GroupingKey.Gender]
    hashcoder = merge.create_grouping_hashcoder(attributes)

    assert callable(hashcoder)

    item: interface.ProtocolSegment = interface.ProtocolSegment(
        content_type=interface.ContentType.TaggedFrame,
        id="a",
        name="apa",
        page_number="0",
        data="hej",
        who="alexis_bjorkman_7f7c23",
        year=1955,
    )
    hashcode = hashcoder(item, member_index['alexis_bjorkman_7f7c23'], source_index)

    assert hashcode is not None


def test_parliamentary_index():

    member_index = member.ParliamentaryMemberIndex()

    assert isinstance(member_index.members, dict)
    assert len(member_index.members) > 0


def test_segment_merger_merge(source_index, member_index):

    # filenames: List[str] = glob.glob('tests/test_data/source/**/prot-*.xml', recursive=True)
    filenames: List[str] = glob.glob(f'{TEST_CORPUS_FOLDER}/**/prot-*.xml', recursive=True)

    texts: Iterable[interface.ProtocolSegment] = parlaclarin.XmlUntangleSegmentIterator(
        filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=0, multiproc_processes=None
    )

    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=source_index,
        member_index=member_index,
        temporal_key=interface.TemporalKey.Year,
        grouping_keys=[interface.GroupingKey.Party],
    )

    assert merger is not None

    groups: Mapping[str, merge.MergedSegmentGroup] = []
    for item in merger.merge(texts):
        groups.append(item)

    assert len(groups) > 0


def test_extract_corpus_text_yearly_grouped_by_party():

    opts = {
        'source_folder': 'tests/test_data/source',
        'target_name': f'tests/output/{uuid.uuid1()}.zip',
        'target_type': 'zip',
        'segment_level': interface.SegmentLevel.Who,
        'years': None,
        'temporal_key': interface.TemporalKey.Year,
        'group_keys': [interface.GroupingKey.Party],
    }

    parlaclarin.extract_corpus_text(**opts)

    assert os.path.isfile(opts['target_name'])

    os.unlink(opts['target_name'])


# @pytest.mark.xfail
def test_extract_corpus_with_no_temporal_key():

    opts = {
        'source_folder': 'tests/test_data/source',
        'target_name': f'tests/output/{uuid.uuid1()}.zip',
        'target_type': 'zip',
        'segment_level': interface.SegmentLevel.Who,
        'years': None,
        'temporal_key': None,
        'group_keys': [interface.GroupingKey.Party],
    }

    parlaclarin.extract_corpus_text(**opts)

    assert os.path.isfile(opts['target_name'])

    os.unlink(opts['target_name'])


def test_extract_corpus_with_no_matching_protocols():

    opts = {
        'source_folder': 'tests/test_data/source',
        'target_name': f'tests/output/{uuid.uuid1()}.zip',
        'target_type': 'zip',
        'segment_level': interface.SegmentLevel.Who,
        'years': '1900',
        'temporal_key': interface.TemporalKey.Year,
        'group_keys': [interface.GroupingKey.Party],
    }

    parlaclarin.extract_corpus_text(**opts)

    assert os.path.isfile(opts['target_name'])

    os.unlink(opts['target_name'])


def test_aggregator_extract_gender_party_no_temporal_key():

    target_filename: str = f'tests/output/{uuid.uuid1()}.zip'
    opts = {
        'source_folder': 'tests/test_data/source',
        'target_name': target_filename,
        'target_type': 'zip',
        'segment_level': interface.SegmentLevel.Who,
        'temporal_key': None,
        'group_keys': (interface.GroupingKey.Party, interface.GroupingKey.Gender),
        'years': '1955-1965',
        'segment_skip_size': 1,
        'multiproc_keep_order': False,
        'multiproc_processes': None,
        'dedent': False,
        'dehyphen': False,
        '_': {},
    }

    parlaclarin.extract_corpus_text(**opts)

    assert os.path.isfile(target_filename)

    os.unlink(target_filename)

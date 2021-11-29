import os
import uuid
from typing import Iterable, List

import pytest

from pyriksprot import CorpusSourceIndex, dispatch, interface, member, tagged_corpus

# pylint: disable=redefined-outer-name


# TEST_CORPUS_FOLDER = '/data/riksdagen_corpus_data/annotated'
TEST_CORPUS_FOLDER = 'tests/test_data/tagged'


@pytest.fixture
def member_index() -> member.ParliamentaryMemberIndex:
    return member.ParliamentaryMemberIndex(f'{TEST_CORPUS_FOLDER}/members_of_parliament.csv')


def test_glob_protocols():
    corpus_source: str = './tests/test_data/tagged'
    filenames: List[str] = tagged_corpus.glob_protocols(corpus_source, file_pattern='prot-*.zip', strip_path=True)
    assert len(filenames) == 20
    """Empty files should be included"""
    assert 'prot-1973--21.zip' in filenames


def test_create_source_index_for_tagged_corpus():
    corpus_source: str = './tests/test_data/tagged'
    source_index = CorpusSourceIndex.load(source_folder=corpus_source, source_pattern='**/prot-*.zip')
    assert isinstance(source_index, CorpusSourceIndex)
    assert len(source_index) == 20


def test_load_protocols():
    corpus_source: str = './tests/test_data/tagged'
    filenames: List[str] = tagged_corpus.glob_protocols(corpus_source, file_pattern='prot-*.zip')

    protocol_iter: Iterable[interface.Protocol] = tagged_corpus.load_protocols(corpus_source)
    protocols = list(protocol_iter)

    """Empty files should NOT be included"""
    assert len(protocols) == len(filenames) - 1


@pytest.mark.parametrize('temporal_key, group_keys', [(interface.TemporalKey.Year, [interface.GroupingKey.Party])])
def test_extract_corpus_tags_yearly_grouped(temporal_key, group_keys):

    target_name = f'tests/output/{temporal_key}_{"_".join(group_keys)}_{uuid.uuid1()}.zip'
    opts = dict(
        source_folder='./tests/test_data/tagged',
        target_name=target_name,
        target_type=dispatch.TargetType.Zip,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Who,
        temporal_key=temporal_key,
        group_keys=group_keys,
        multiproc_keep_order=None,
        multiproc_processes=1,
        years=None,
        segment_skip_size=1,
    )

    tagged_corpus.extract_corpus_tags(**opts)

    assert os.path.isfile(opts['target_name'])

    # os.unlink(opts['target_name'])


# # @pytest.mark.xfail
# def test_extract_corpus_with_no_temporal_key():

#     opts = {
#         'source_folder': 'tests/test_data/source',
#         'target_name': f'tests/output/{uuid.uuid1()}.zip',
#         'target_type': dispatch.TargetType.Zip,
#         'segment_level': interface.SegmentLevel.Who,
#         'temporal_key': None,
#         'group_keys': [interface.GroupingKey.Party],
#         'years': None,
#     }

#     pyriksprot.extract_corpus_text(**opts)

#     assert os.path.isfile(opts['target_name'])

#     os.unlink(opts['target_name'])


# def test_extract_corpus_with_no_matching_protocols():

#     opts = {
#         'source_folder': 'tests/test_data/source',
#         'target_name': f'tests/output/{uuid.uuid1()}.zip',
#         'target_type': dispatch.TargetType.Zip,
#         'segment_level': interface.SegmentLevel.Who,
#         'temporal_key': interface.TemporalKey.Year,
#         'group_keys': [interface.GroupingKey.Party],
#         'years': '1900',
#     }

#     pyriksprot.extract_corpus_text(**opts)

#     assert os.path.isfile(opts['target_name'])

#     os.unlink(opts['target_name'])


# def test_aggregator_extract_gender_party_no_time_period():

#     target_filename: str = f'tests/output/{uuid.uuid1()}.zip'
#     opts = {
#         'source_folder': 'tests/test_data/source',
#         'target_name': target_filename,
#         'target_type': dispatch.TargetType.Zip,
#         'segment_level': interface.SegmentLevel.Who,
#         'temporal_key': None,
#         'group_keys': (interface.GroupingKey.Party, interface.GroupingKey.Gender),
#         'years': '1955-1965',
#         'segment_skip_size': 1,
#         'multiproc_keep_order': False,
#         'multiproc_processes': None,
#         '_': {},
#     }

#     pyriksprot.extract_corpus_text(**opts)

#     assert os.path.isfile(target_filename)

#     os.unlink(target_filename)

import glob
import os
import uuid
from typing import Iterable, List, Mapping

import pandas as pd
import pytest

from pyriksprot import corpus_index, dispatch, interface, member, merge, parlaclarin

from ..utility import PARLACLARIN_SOURCE_BRANCH, PARLACLARIN_SOURCE_FOLDER, PARLACLARIN_SOURCE_PATTERN

# pylint: disable=redefined-outer-name


@pytest.fixture
def source_index() -> corpus_index.CorpusSourceIndex:

    items: corpus_index.CorpusSourceIndex = corpus_index.CorpusSourceIndex.load(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        source_pattern='**/prot-*.xml',
        skip_empty=False,
    )
    return items


def test_create_grouping_hashcoder(
    source_index: corpus_index.CorpusSourceIndex, member_index: member.ParliamentaryMemberIndex
):

    attributes = [interface.SegmentLevel.Who, interface.GroupingKey.Gender]
    hashcoder = merge.create_grouping_hashcoder(attributes)

    assert callable(hashcoder)

    item: interface.ProtocolSegment = interface.ProtocolSegment(
        protocol_name="apa",
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

    member_index = member.ParliamentaryMemberIndex(branch=PARLACLARIN_SOURCE_BRANCH)

    assert isinstance(member_index.members, pd.DataFrame)
    assert len(member_index.members) > 0


def test_segment_merger_merge(source_index, member_index):

    filenames: List[str] = glob.glob(PARLACLARIN_SOURCE_PATTERN, recursive=True)

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

    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    parlaclarin.extract_corpus_text(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        target_name=target_name,
        target_type='files-in-zip',
        compress_type=dispatch.CompressType.Zip,
        segment_level=interface.SegmentLevel.Who,
        years=None,
        temporal_key=interface.TemporalKey.Year,
        group_keys=[interface.GroupingKey.Party],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


# @pytest.mark.xfail
def test_extract_corpus_with_no_temporal_key():
    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    parlaclarin.extract_corpus_text(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        target_name=target_name,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        years=None,
        temporal_key=None,
        group_keys=[interface.GroupingKey.Party],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


def test_extract_corpus_with_no_matching_protocols():
    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    parlaclarin.extract_corpus_text(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        target_name=target_name,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        years='1900',
        temporal_key=interface.TemporalKey.Year,
        group_keys=[interface.GroupingKey.Party],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


def test_aggregator_extract_gender_party_no_temporal_key():

    target_filename: str = f'tests/output/{uuid.uuid1()}.zip'

    parlaclarin.extract_corpus_text(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        target_name=target_filename,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        temporal_key=None,
        group_keys=(interface.GroupingKey.Party, interface.GroupingKey.Gender),
        years='1955-1965',
        segment_skip_size=1,
        multiproc_keep_order=False,
        multiproc_processes=None,
        dedent=False,
        dehyphen=False,
        _={},
    )

    assert os.path.isfile(target_filename)

    os.unlink(target_filename)

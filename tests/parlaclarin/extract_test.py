import glob
import os
import uuid
from typing import Iterable

import pytest

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import workflows
from pyriksprot.corpus import corpus_index as csi
from pyriksprot.corpus import iterate, parlaclarin
from pyriksprot.dispatch import dispatch, merge

from ..utility import (
    RIKSPROT_PARLACLARIN_FOLDER,
    RIKSPROT_PARLACLARIN_PATTERN,
    RIKSPROT_REPOSITORY_TAG,
    TAGGED_METADATA_DATABASE_NAME,
)


def test_create_grouping_hashcoder():
    protocol_name: str = "prot-1955--ak--22"
    person_id: str = "Q5715273"
    u_id: str = "d68df3cd45d2eec6-0"
    item: iterate.ProtocolSegment = iterate.ProtocolSegment(
        protocol_name=protocol_name,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        id=u_id,
        u_id=u_id,
        name=u_id,
        page_number="0",
        data="hej",
        who=person_id,
        year=1955,
        n_tokens=0,
    )
    # source_item = source_index.lookup.get("prot-1955--ak--22")
    source_item: csi.CorpusSourceItem = csi.CorpusSourceItem(
        path=f'tests/test_data/source/tagged_frames/{RIKSPROT_REPOSITORY_TAG}/prot-1955--ak--22.zip',
        filename='prot-1955--ak--22.zip',
        name='prot-1955--ak--22',
        subfolder=RIKSPROT_REPOSITORY_TAG,
        year=1955,
        metadata={
            'name': 'prot-1955--ak--22',
            'date': '1955-05-20',
            'checksum': '560f443658031647fbe1d3f88cdd60b515b1dbba',
        },
    )
    # speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)
    speaker: md.SpeakerInfo = md.SpeakerInfo(
        speech_id='d68df3cd45d2eec6-0',
        person_id='Q5715273',
        name='Ericsson',
        gender_id=1,
        party_id=8,
        start_year=1937,
        end_year=1959,
        office_type_id=1,
        sub_office_type_id=2,
    )
    with pytest.raises(ValueError):
        _ = merge.create_grouping_hashcoder(["dummy_id"])

    hashcoder = merge.create_grouping_hashcoder([])

    item.speaker_info = speaker
    parts, hash_str, _ = hashcoder(item=item, source_item=None)

    assert not parts
    assert hash_str == item.name

    attributes: list[str] = ["who", "gender_id", "party_id", "office_type_id"]
    hashcoder = merge.create_grouping_hashcoder(attributes)
    parts, hash_str, _ = hashcoder(item=item, source_item=source_item)

    assert parts == {
        'gender_id': str(speaker.gender_id),
        'office_type_id': str(speaker.office_type_id),
        'party_id': str(speaker.party_id),
        'who': item.who,
    }
    assert set(hash_str.split("_")) == set('1_1_8_q5715273'.split("_"))


def test_segment_merger_merge(xml_source_index: csi.CorpusSourceIndex):
    speaker: md.SpeakerInfo = md.SpeakerInfo(
        speech_id='dummy-0',
        person_id='Q123456',
        name='Dummy',
        gender_id=1,
        party_id=8,
        start_year=1937,
        end_year=1959,
        office_type_id=1,
        sub_office_type_id=2,
    )

    def assign_speaker(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker

    filenames: list[str] = glob.glob(RIKSPROT_PARLACLARIN_PATTERN, recursive=True)

    texts: Iterable[iterate.ProtocolSegment] = parlaclarin.XmlUntangleSegmentIterator(
        filenames=filenames,
        segment_level=interface.SegmentLevel.Who,
        segment_skip_size=0,
        multiproc_processes=None,
        preprocess=assign_speaker,
    )

    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=xml_source_index,
        temporal_key=interface.TemporalKey.Year,
        grouping_keys=["gender_id", "party_id"],
    )

    assert merger is not None
    assert merger.grouping_keys == ["gender_id", "party_id"]

    groups: list[dict[str, merge.DispatchItem]] = [item for item in merger.merge(texts)]

    assert len(groups) > 0
    g: dict[str, merge.DispatchItem] = groups[0]
    key = list(g.keys())[0]  # '72e6f6e0f08ca88f02b1480464afd55b'
    data = g[key]
    # FIXME: 'who' is added to values (bugfix)
    # assert set(data.grouping_keys) == {'gender_id', 'party_id'}
    assert set(data.group_values.keys()) == {'gender_id', 'party_id', 'who'}


def test_extract_corpus_text_yearly_grouped_by_party():

    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    workflows.extract_corpus_text(
        source_folder=RIKSPROT_PARLACLARIN_FOLDER,
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
        target_name=target_name,
        target_type='files-in-zip',
        compress_type=dispatch.CompressType.Zip,
        segment_level=interface.SegmentLevel.Who,
        years=None,
        temporal_key=interface.TemporalKey.Year,
        group_keys=[interface.GroupingKey.party_id],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


# @pytest.mark.xfail
def test_extract_corpus_with_no_temporal_key():
    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    workflows.extract_corpus_text(
        source_folder=RIKSPROT_PARLACLARIN_FOLDER,
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
        target_name=target_name,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        years=None,
        temporal_key=None,
        group_keys=[interface.GroupingKey.party_id],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


def test_extract_corpus_with_no_matching_protocols():
    target_name: str = f'tests/output/{uuid.uuid1()}.zip'

    workflows.extract_corpus_text(
        source_folder=RIKSPROT_PARLACLARIN_FOLDER,
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
        target_name=target_name,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        years='1900',
        temporal_key=interface.TemporalKey.Year,
        group_keys=[interface.GroupingKey.party_id],
    )

    assert os.path.isfile(target_name)

    os.unlink(target_name)


def test_aggregator_extract_gender_party_no_temporal_key():

    target_filename: str = f'tests/output/{uuid.uuid1()}.zip'

    workflows.extract_corpus_text(
        source_folder=RIKSPROT_PARLACLARIN_FOLDER,
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
        target_name=target_filename,
        target_type='files-in-zip',
        segment_level=interface.SegmentLevel.Who,
        temporal_key=None,
        group_keys=(interface.GroupingKey.party_id, interface.GroupingKey.gender_id),
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

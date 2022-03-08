import glob
import os
import uuid
from typing import Iterable, List, Mapping

from pyriksprot import corpus_index as csi
from pyriksprot import dispatch, interface, merge
from pyriksprot import metadata as md
from pyriksprot import parlaclarin, segment

from ..utility import PARLACLARIN_SOURCE_FOLDER, PARLACLARIN_SOURCE_PATTERN, TAGGED_METADATA_DATABASE_NAME

# pylint: disable=redefined-outer-name


def test_create_grouping_hashcoder(source_index: csi.CorpusSourceIndex, speaker_service: md.SpeakerInfoService):

    attributes = [interface.SegmentLevel.Who, interface.GroupingKey.Gender]
    hashcoder = merge.create_grouping_hashcoder(attributes)

    assert callable(hashcoder)

    item: segment.ProtocolSegment = segment.ProtocolSegment(
        protocol_name="apa",
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        id="a",
        u_id="a",
        name="apa",
        page_number="0",
        data="hej",
        who="Q5715273",
        year=1955,
    )
    speaker: md.SpeakerInfo = speaker_service.get_speaker_info(u_id=item.u_id, person_id=item.who, year=item.year)
    hashcode = hashcoder(item, speaker, source_index)

    assert hashcode is not None


def test_segment_merger_merge(xml_source_index: csi.CorpusSourceIndex, speaker_service: md.SpeakerInfoService):

    filenames: List[str] = glob.glob(PARLACLARIN_SOURCE_PATTERN, recursive=True)

    texts: Iterable[segment.ProtocolSegment] = parlaclarin.XmlUntangleSegmentIterator(
        filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=0, multiproc_processes=None
    )

    merger: merge.SegmentMerger = merge.SegmentMerger(
        source_index=xml_source_index,
        speaker_service=speaker_service,
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
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
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
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
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
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
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
        metadata_filename=TAGGED_METADATA_DATABASE_NAME,
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

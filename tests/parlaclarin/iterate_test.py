import os
from typing import Iterable, Type

import pytest

from pyriksprot import interface, utility
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus import iterate, parlaclarin
from tests.parlaclarin.utility import get_test_filenames

from .. import fakes

jj = os.path.join


# FIXME: Must check expected count for source!!!
@pytest.mark.parametrize(
    'iterator_class, n_speeches, n_missing_intros',
    [
        (parlaclarin.XmlProtocolSegmentIterator, 475, 2),
        (parlaclarin.XmlUntangleSegmentIterator, 475, 2),
    ],
)
def test_segment_iterator_when_segment_is_speech(iterator_class, n_speeches: int, n_missing_intros: int):
    texts: Iterable[iterate.ProtocolSegment] = list(
        iterator_class(
            filenames=get_test_filenames(),
            segment_level=interface.SegmentLevel.Speech,
            segment_skip_size=0,
            multiproc_processes=None,
            content_type=interface.ContentType.Text,
            merge_strategy='chain_consecutive_unknowns',
        )
    )

    assert len(texts) == n_speeches
    assert all(x.u_id for x in texts)
    assert all(x.id == x.u_id for x in texts)
    assert all(x.speaker_note_id for x in texts)

    """This might be data error"""
    assert n_missing_intros == len([x for x in texts if x.speaker_note_id == "missing"])


@pytest.mark.parametrize(
    'iterator_class',
    [
        parlaclarin.XmlProtocolSegmentIterator,
        parlaclarin.XmlUntangleSegmentIterator,
    ],
)
def test_segment_iterator_when_segment_is_protocol(iterator_class):
    filenames: list[str] = get_test_filenames()

    texts: Iterable[iterate.ProtocolSegment] = list(
        iterator_class(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Protocol,
            segment_skip_size=0,
            multiproc_processes=None,
            content_type=interface.ContentType.Text,
        )
    )

    expected_document_names = sorted(utility.strip_path_and_extension(filenames))

    assert [x.name for x in texts] == expected_document_names

    assert not any(x.u_id for x in texts)
    assert not any(x.speaker_note_id for x in texts)

    assert all(x.who is None for x in texts)
    assert [x.id for x in texts] == expected_document_names
    assert all(x.page_number == 0 for x in texts)


def test_xml_protocol_texts_iterator_texts():
    filenames: list[str] = get_test_filenames()
    expected_document_names: list[str] = sorted(utility.strip_path_and_extension(filenames))

    texts: Iterable[iterate.ProtocolSegment] = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None
        )
    )
    p_texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=2
        )
    )

    assert set(p.data for p in p_texts) == set(p.data for p in texts)

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level='protocol',
            segment_skip_size=0,
            multiproc_processes=None,
            multiproc_keep_order=True,
        )
    )
    assert {x.name for x in texts} == set(expected_document_names)

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=None, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 5

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Speech,
            segment_skip_size=1,
            multiproc_processes=None,
            merge_strategy='chain_consecutive_unknowns',
        )
    )
    assert len(texts) == 475

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 149


@pytest.mark.parametrize(
    'iterator_class, document_names, level',
    [(parlaclarin.XmlUntangleSegmentIterator, ['prot-1958-fake', 'prot-1960-fake'], interface.SegmentLevel.Protocol)],
)
def test_protocol_texts_iterator(
    iterator_class: Type[iterate.ProtocolSegmentIterator],
    document_names: list[str],
    level: interface.SegmentLevel.Protocol,
):
    fakes_folder: str = ConfigStore.config().get("fakes:folder")
    filenames: list[str] = [jj(fakes_folder, f"{name}.xml") for name in document_names]

    segments: list[iterate.ProtocolSegment] = list(
        iterator_class(
            filenames=filenames, segment_level=level.name.lower(), segment_skip_size=0, multiproc_processes=None
        )
    )
    """Load truth"""
    expected_stream: list[iterate.ProtocolSegment] = fakes.load_expected_stream(level, document_names)
    assert segments == expected_stream

    segments: list[iterate.ProtocolSegment] = list(
        iterator_class(
            filenames=filenames, segment_level=level.name.lower(), segment_skip_size=0, multiproc_processes=2
        )
    )
    assert set(t.data for t in segments) == set(t.data for t in expected_stream)


@pytest.mark.parametrize(
    'iterator_class, segment_level',
    [
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Protocol),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Speech),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Who),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Utterance),
    ],
)
def test_protocol_texts_iterator_levels(iterator_class, segment_level: interface.SegmentLevel):
    fakes_folder: str = ConfigStore.config().get("fakes:folder")
    document_names: list[str] = ['prot-1958-fake']  # , 'prot-1960-fake']
    filenames: list[str] = [jj(fakes_folder, f"{name}.xml") for name in document_names]
    texts: list[iterate.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None)
    )
    expected_stream: list[iterate.ProtocolSegment] = fakes.load_expected_stream(segment_level, document_names)

    assert len(texts) == len(expected_stream)
    for t, x in zip(texts, expected_stream):
        assert t.protocol_name == x.protocol_name
        assert t.content_type == x.content_type
        assert t.segment_level == x.segment_level
        assert t.name == x.name
        assert t.who == x.who
        assert t.id == x.id
        assert t.u_id == x.u_id
        assert t.data == x.data
        assert t.page_number == x.page_number
        assert t.year == x.year

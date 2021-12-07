import glob
import os
from typing import Iterable, List

import pytest

from pyriksprot import interface, parlaclarin, utility

jj = os.path.join


@pytest.mark.parametrize(
    'iterator_class',
    [
        parlaclarin.XmlProtocolSegmentIterator,
        parlaclarin.XmlUntangleSegmentIterator,
        parlaclarin.XmlSaxSegmentIterator,
    ],
)
def test_protocol_texts_iterator_metadata(iterator_class):

    filenames = glob.glob('tests/test_data/source/**/prot-*.xml', recursive=True)
    expected_document_names = sorted(utility.strip_path_and_extension(filenames))

    texts: Iterable[interface.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None)
    )

    assert len(texts) == 6
    assert [x.name for x in texts] == expected_document_names
    assert all(x.who is None for x in texts)
    assert [x.id for x in texts] == expected_document_names
    assert all(x.page_number == '0' for x in texts)


def test_xml_protocol_texts_iterator_texts():

    filenames: List[str] = glob.glob('tests/test_data/source/**/prot-*.xml', recursive=True)
    expected_document_names: List[str] = sorted(utility.strip_path_and_extension(filenames))

    texts: Iterable[interface.ProtocolSegment] = list(
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
            multiproc_processes=2,
            multiproc_keep_order=True,
        )
    )
    assert [x.name for x in texts] == expected_document_names

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=None, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 6

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Speech,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    assert len(texts) == 110

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 110

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Utterance,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Utterance,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))

    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)


EXPECTED_STREAM = {
    'protocol': [
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake',
            None,
            'prot-1958-fake',
            'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nJag heter Adam.\nOve är dum.',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake',
            None,
            'prot-1960-fake',
            'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?\nJag håller med.\nTalmannen är snäll.\nJag håller också med.',
            '0',
            1960,
        ),
    ],
    'speech': [
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-001',
            'A',
            'c01',
            'Hej! Detta är en mening.',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-002',
            'A',
            'c02',
            'Jag heter Ove.\nVad heter du?',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-003',
            'B',
            'c03',
            'Jag heter Adam.\nOve är dum.',
            '1',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-004',
            'A',
            'c01',
            'Herr Talman! Jag talar.',
            '0',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-005',
            'A',
            'c02',
            'Det regnar ute.\nVisste du det?',
            '0',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-006',
            'B',
            'c03',
            'Jag håller med.\nTalmannen är snäll.',
            '1',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-007',
            'C',
            'c04',
            'Jag håller också med.',
            '1',
            1960,
        ),
    ],
    'who': [
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-001',
            'A',
            'A',
            'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-002',
            'B',
            'B',
            'Jag heter Adam.\nOve är dum.',
            '1',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-003',
            'A',
            'A',
            'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?',
            '0',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-004',
            'B',
            'B',
            'Jag håller med.\nTalmannen är snäll.',
            '1',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-005',
            'C',
            'C',
            'Jag håller också med.',
            '1',
            1960,
        ),
    ],
    'utterance': [
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-001',
            'A',
            'i-1',
            'Hej! Detta är en mening.',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-002',
            'A',
            'i-2',
            'Jag heter Ove.\nVad heter du?',
            '0',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-003',
            'B',
            'i-3',
            'Jag heter Adam.',
            '1',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1958-fake',
            interface.ContentType.Text,
            'prot-1958-fake-004',
            'B',
            'i-4',
            'Ove är dum.',
            '1',
            1958,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-005',
            'A',
            'i-1',
            'Herr Talman! Jag talar.',
            '0',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-006',
            'A',
            'i-2',
            'Det regnar ute.\nVisste du det?',
            '0',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-007',
            'B',
            'i-3',
            'Jag håller med.\nTalmannen är snäll.',
            '1',
            1960,
        ),
        interface.ProtocolSegment(
            'prot-1960-fake',
            interface.ContentType.Text,
            'prot-1960-fake-008',
            'C',
            'i-4',
            'Jag håller också med.',
            '1',
            1960,
        ),
    ],
}


@pytest.mark.parametrize(
    'iterator_class',
    [
        # DEPRECATED parlaclarin.XmlProtocolSegmentIterator,
        parlaclarin.XmlUntangleSegmentIterator,
        # DEPRECATED parlaclarin.XmlSaxSegmentIterator,
    ],
)
def test_protocol_texts_iterator(iterator_class):

    document_names: List[str] = ['prot-1958-fake', 'prot-1960-fake']
    filenames: List[str] = [jj("tests", "test_data", "fake", f"{name}.xml") for name in document_names]

    segments: List[interface.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None)
    )
    assert segments == EXPECTED_STREAM['protocol']

    segments: List[interface.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=2)
    )
    assert set(t.data for t in segments) == set(t.data for t in EXPECTED_STREAM['protocol'])


@pytest.mark.parametrize(
    'segment_level',
    [
        interface.SegmentLevel.Protocol,
        interface.SegmentLevel.Speech,
        interface.SegmentLevel.Who,
        interface.SegmentLevel.Utterance,
    ],
)
def test_protocol_texts_iterator_levels_compare(segment_level):

    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None
        )
    )
    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)

    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None
        )
    )
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)

    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))


@pytest.mark.parametrize(
    'iterator_class, segment_level',
    [
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Protocol),
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Speech), # FIXME! Chain not working!!!!
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Who),
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Utterance),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Protocol),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Speech),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Who),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Utterance),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Protocol),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Speech),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Who),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Utterance),
    ],
)
def test_protocol_texts_iterator_levels(iterator_class, segment_level: interface.SegmentLevel):

    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]
    texts = list(
        iterator_class(filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None)
    )
    assert texts == EXPECTED_STREAM[segment_level]

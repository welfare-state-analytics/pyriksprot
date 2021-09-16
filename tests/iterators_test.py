import os
from typing import Iterable

import pytest

from pyriksprot import iterators
from pyriksprot.interface import ProtocolIterItem

jj = os.path.join

DOCUMENT_NAMES = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
]


@pytest.mark.parametrize(
    'iterator_class',
    [
        iterators.ProtocolTextIterator,
        iterators.XmlProtocolTextIterator,
        iterators.XmlIterProtocolTextIterator,
    ],
)
def test_protocol_texts_iterator_metadata(iterator_class):

    filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]

    texts: Iterable[ProtocolIterItem] = list(
        iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=None)
    )

    assert len(texts) == 4
    assert [x.name for x in texts] == DOCUMENT_NAMES
    assert all(x.who is None for x in texts)
    assert [x.id for x in texts] == DOCUMENT_NAMES
    assert all(x.page_number == '0' for x in texts)


def test_xml_protocol_texts_iterator_texts():

    filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]

    texts: Iterable[ProtocolIterItem] = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None)
    )
    p_texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2))

    assert set(p.text for p in p_texts) == set(p.text for p in texts)

    texts = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2, ordered=True)
    )
    assert [x.name for x in texts] == DOCUMENT_NAMES

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=1, processes=None))
    assert len(texts) == 4

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speech', skip_size=1, processes=None))
    assert len(texts) == 34

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None))
    assert len(texts) == 34

    texts1 = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='utterance', skip_size=1, processes=None)
    )
    texts2 = list(
        iterators.XmlIterProtocolTextIterator(filenames=filenames, level='utterance', skip_size=1, processes=None)
    )
    assert all(''.join(texts1[i].text.split()) == ''.join(texts2[i].text.split()) for i in range(0, len(texts1)))

    texts1 = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None))
    texts2 = list(
        iterators.XmlIterProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None)
    )
    assert all(''.join(texts1[i].text.split()) == ''.join(texts2[i].text.split()) for i in range(0, len(texts1)))

    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)


EXPECTED_STREAM = {
    'protocol': [
        ProtocolIterItem(
            'prot-1958-fake',
            None,
            'prot-1958-fake',
            'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nJag heter Adam.\nOve är dum.',
            '0',
        ),
        ProtocolIterItem(
            'prot-1960-fake',
            None,
            'prot-1960-fake',
            'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?\nJag håller med.\nTalmannen är snäll.\nJag håller också med.',
            '0',
        ),
    ],
    'speech': [
        ProtocolIterItem('prot-1958-fake', 'A', 'c01', 'Hej! Detta är en mening.', '0'),
        ProtocolIterItem('prot-1958-fake', 'A', 'c02', 'Jag heter Ove.\nVad heter du?', '0'),
        ProtocolIterItem('prot-1958-fake', 'B', 'c03', 'Jag heter Adam.\nOve är dum.', '1'),
        ProtocolIterItem('prot-1960-fake', 'A', 'c01', 'Herr Talman! Jag talar.', '0'),
        ProtocolIterItem('prot-1960-fake', 'A', 'c02', 'Det regnar ute.\nVisste du det?', '0'),
        ProtocolIterItem('prot-1960-fake', 'B', 'c03', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ProtocolIterItem('prot-1960-fake', 'C', 'c04', 'Jag håller också med.', '1'),
    ],
    'speaker': [
        ProtocolIterItem('prot-1958-fake', 'A', 'A', 'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?', '0'),
        ProtocolIterItem('prot-1958-fake', 'B', 'B', 'Jag heter Adam.\nOve är dum.', '1'),
        ProtocolIterItem('prot-1960-fake', 'A', 'A', 'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?', '0'),
        ProtocolIterItem('prot-1960-fake', 'B', 'B', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ProtocolIterItem('prot-1960-fake', 'C', 'C', 'Jag håller också med.', '1'),
    ],
    'utterance': [
        ProtocolIterItem('prot-1958-fake', 'A', 'i-1', 'Hej! Detta är en mening.', '0'),
        ProtocolIterItem('prot-1958-fake', 'A', 'i-2', 'Jag heter Ove.\nVad heter du?', '0'),
        ProtocolIterItem('prot-1958-fake', 'B', 'i-3', 'Jag heter Adam.', '1'),
        ProtocolIterItem('prot-1958-fake', 'B', 'i-4', 'Ove är dum.', '1'),
        ProtocolIterItem('prot-1960-fake', 'A', 'i-1', 'Herr Talman! Jag talar.', '0'),
        ProtocolIterItem('prot-1960-fake', 'A', 'i-2', 'Det regnar ute.\nVisste du det?', '0'),
        ProtocolIterItem('prot-1960-fake', 'B', 'i-3', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ProtocolIterItem('prot-1960-fake', 'C', 'i-4', 'Jag håller också med.', '1'),
    ],
}


@pytest.mark.parametrize(
    'iterator_class',
    [
        iterators.ProtocolTextIterator,
        iterators.XmlProtocolTextIterator,
        iterators.XmlIterProtocolTextIterator,
    ],
)
def test_protocol_texts_iterator(iterator_class):

    # filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]
    document_names = ['prot-1958-fake', 'prot-1960-fake']
    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in document_names]

    texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=None))
    assert texts == EXPECTED_STREAM['protocol']

    texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=2))
    assert set(t.text for t in texts) == set(t.text for t in EXPECTED_STREAM['protocol'])


@pytest.mark.parametrize(
    'level',
    [
        'protocol',
        'speech',
        'speaker',
        'utterance',
    ],
)
def test_protocol_texts_iterator_levels_compare(level):

    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]

    texts1 = list(iterators.XmlProtocolTextIterator(filenames=filenames, level=level, skip_size=1, processes=None))
    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)

    texts2 = list(iterators.XmlIterProtocolTextIterator(filenames=filenames, level=level, skip_size=1, processes=None))
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)

    assert all(''.join(texts1[i].text.split()) == ''.join(texts2[i].text.split()) for i in range(0, len(texts1)))


@pytest.mark.parametrize(
    'iterator_class, level',
    [
        (iterators.ProtocolTextIterator, 'protocol'),
        (iterators.ProtocolTextIterator, 'speech'),
        (iterators.ProtocolTextIterator, 'speaker'),
        (iterators.ProtocolTextIterator, 'utterance'),
        (iterators.XmlProtocolTextIterator, 'protocol'),
        (iterators.XmlProtocolTextIterator, 'speech'),
        (iterators.XmlProtocolTextIterator, 'speaker'),
        (iterators.XmlProtocolTextIterator, 'utterance'),
        (iterators.XmlIterProtocolTextIterator, 'protocol'),
        (iterators.XmlIterProtocolTextIterator, 'speech'),
        (iterators.XmlIterProtocolTextIterator, 'speaker'),
        (iterators.XmlIterProtocolTextIterator, 'utterance'),
    ],
)
def test_protocol_texts_iterator_levels(iterator_class, level):

    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]
    texts = list(iterator_class(filenames=filenames, level=level, skip_size=1, processes=None))
    assert texts == EXPECTED_STREAM[level]

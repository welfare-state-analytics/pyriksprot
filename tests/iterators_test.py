import os

import pytest

from pyriksprot import iterators

jj = os.path.join

DOCUMENT_NAMES = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
]


def test_xml_protocol_texts_iterator():

    filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None))

    assert len(texts) == 4
    assert [x[0] for x in texts] == DOCUMENT_NAMES
    assert all(x[1] is None for x in texts)
    assert [x[2] for x in texts] == DOCUMENT_NAMES
    assert all(x[4] == '0' for x in texts)
    # assert [len(x[3]) for x in texts] == [189, 636693, 0, 0]

    p_texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2))

    assert set(p_texts) == set(texts)

    texts = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2, ordered=True)
    )
    assert [x[0] for x in texts] == DOCUMENT_NAMES

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=1, processes=None))
    assert len(texts) == 4

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speech', skip_size=1, processes=None))
    assert len(texts) == 34

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None))
    assert len(texts) == 34

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='utterance', skip_size=1, processes=None))
    assert len(texts) == 378


EXPECTED_STREAM = {
    'protocol': [
        (
            'prot-1958-fake',
            None,
            'prot-1958-fake',
            'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nJag heter Adam.\nOve är dum.',
            '0',
        ),
        (
            'prot-1960-fake',
            None,
            'prot-1960-fake',
            'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?\nJag håller med.\nTalmannen är snäll.\nJag håller också med.',
            '0',
        ),
    ],
    'speech': [
        ('prot-1958-fake', 'A', 'c01', 'Hej! Detta är en mening.', '0'),
        ('prot-1958-fake', 'A', 'c02', 'Jag heter Ove.\nVad heter du?', '0'),
        ('prot-1958-fake', 'B', 'c03', 'Jag heter Adam.\nOve är dum.', '1'),
        ('prot-1960-fake', 'A', 'c01', 'Herr Talman! Jag talar.', '0'),
        ('prot-1960-fake', 'A', 'c02', 'Det regnar ute.\nVisste du det?', '0'),
        ('prot-1960-fake', 'B', 'c03', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ('prot-1960-fake', 'C', 'c04', 'Jag håller också med.', '1'),
    ],
    'speaker': [
        ('prot-1958-fake', 'A', 'A', 'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?', '0'),
        ('prot-1958-fake', 'B', 'B', 'Jag heter Adam.\nOve är dum.', '1'),
        ('prot-1960-fake', 'A', 'A', 'Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?', '0'),
        ('prot-1960-fake', 'B', 'B', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ('prot-1960-fake', 'C', 'C', 'Jag håller också med.', '1'),
    ],
    'utterance': [
        ('prot-1958-fake', 'A', 'i-1', 'Hej! Detta är en mening.', '0'),
        ('prot-1958-fake', 'A', 'i-2', 'Jag heter Ove.\nVad heter du?', '0'),
        ('prot-1958-fake', 'B', 'i-3', 'Jag heter Adam.', '1'),
        ('prot-1958-fake', 'B', 'i-4', 'Ove är dum.', '1'),
        ('prot-1960-fake', 'A', 'i-1', 'Herr Talman! Jag talar.', '0'),
        ('prot-1960-fake', 'A', 'i-2', 'Det regnar ute.\nVisste du det?', '0'),
        ('prot-1960-fake', 'B', 'i-3', 'Jag håller med.\nTalmannen är snäll.', '1'),
        ('prot-1960-fake', 'C', 'i-4', 'Jag håller också med.', '1'),
    ],
}


@pytest.mark.parametrize(
    'iterator_class',
    [
        iterators.ProtocolTextIterator,
        iterators.XmlProtocolTextIterator,
    ],
)
def test_protocol_texts_iterator(iterator_class):

    # filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]
    document_names = ['prot-1958-fake', 'prot-1960-fake']
    filenames = [jj("tests", "test_data", "fake", f"{name}.xml") for name in document_names]

    texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=None))
    assert texts == EXPECTED_STREAM['protocol']

    texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=2))
    assert texts == EXPECTED_STREAM['protocol']

    # texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=0, processes=2, ordered=True))
    # assert texts == EXPECTED_STREAM['protocol']

    texts = list(iterator_class(filenames=filenames, level='protocol', skip_size=1, processes=None))
    assert texts == EXPECTED_STREAM['protocol']

    texts = list(iterator_class(filenames=filenames, level='speech', skip_size=0, processes=None))
    assert texts == EXPECTED_STREAM['speech']

    texts = list(iterator_class(filenames=filenames, level='speaker', skip_size=0, processes=None))
    assert texts == EXPECTED_STREAM['speaker']

    texts = list(iterator_class(filenames=filenames, level='utterance', skip_size=0, processes=None))
    assert texts == EXPECTED_STREAM['utterance']

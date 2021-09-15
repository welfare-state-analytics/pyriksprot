import os

from pyriksprot import iterators

jj = os.path.join

DOCUMENT_NAMES = [
    "prot-1933--fk--5",
    "prot-1955--ak--22",
    "prot-197879--14",
    "prot-199596--35",
]


def test_xml_protocol_texts_iterator_fake_xml():

    filenames: str = [jj("tests", "test_data", "fake", "prot-1958-fake.xml")]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None))

    assert len(texts) == 1
    assert texts[0][0] == "prot-1958-fake"

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None))

    assert len(texts) == 1
    assert texts[0][0] == "prot-1958-fake"

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speaker', skip_size=0, processes=None))

    assert texts == [
        ('prot-1958-fake', 'A', 'A', 'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?'),
        ('prot-1958-fake', 'B', 'B', 'Jag heter Adam.\nOve är dum.'),
    ]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speech', skip_size=0, processes=None))
    assert texts == [
        ('prot-1958-fake', 'A', 'c01', 'Hej! Detta är en mening.'),
        ('prot-1958-fake', 'A', 'c02', 'Jag heter Ove.\nVad heter du?'),
        ('prot-1958-fake', 'B', 'c03', 'Jag heter Adam.\nOve är dum.'),
    ]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='utterance', skip_size=0, processes=None))
    assert texts == [
        ('prot-1958-fake', 'A', 'i-1', 'Hej! Detta är en mening.'),
        ('prot-1958-fake', 'A', 'i-2', 'Jag heter Ove.\nVad heter du?'),
        ('prot-1958-fake', 'B', 'i-3', 'Jag heter Adam.'),
        ('prot-1958-fake', 'B', 'i-4', 'Ove är dum.'),
    ]

    texts = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='paragraphs', skip_size=0, processes=None)
    )
    assert texts == [
        ('prot-1958-fake', 'A', 'i-1@0', 'Hej! Detta är en mening.'),
        ('prot-1958-fake', 'A', 'i-2@0', 'Jag heter Ove.'),
        ('prot-1958-fake', 'A', 'i-2@1', 'Vad heter du?'),
        ('prot-1958-fake', 'B', 'i-3@0', 'Jag heter Adam.'),
        ('prot-1958-fake', 'B', 'i-4@0', 'Ove är dum.'),
    ]


def test_xml_protocol_texts_iterator():

    filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None))

    assert [x[0] for x in texts] == DOCUMENT_NAMES
    assert all(x[1] is None for x in texts)
    assert [x[2] for x in texts] == DOCUMENT_NAMES
    assert [len(x[3]) for x in texts] == [189, 636693, 0, 0]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2))

    assert {x[0] for x in texts} == set(DOCUMENT_NAMES)
    assert all(x[1] is None for x in texts)
    assert {x[2] for x in texts} == set(DOCUMENT_NAMES)
    assert {len(x[3]) for x in texts} == {189, 636693, 0, 0}

    texts = list(
        iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2, ordered=True)
    )
    assert [x[0] for x in texts] == DOCUMENT_NAMES

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='protocol', skip_size=1, processes=None))

    assert [len(x[3]) for x in texts] == [189, 636693]

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speech', skip_size=1, processes=None))

    assert len(texts) == 33

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None))

    assert len(texts) == 33

    texts = list(iterators.XmlProtocolTextIterator(filenames=filenames, level='utterance', skip_size=1, processes=None))

    assert len(texts) == 369


def test_protocol_texts_iterator():

    filenames = [jj("tests", "test_data", "xml", f"{name}.xml") for name in DOCUMENT_NAMES]

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=None))

    assert [x[0] for x in texts] == DOCUMENT_NAMES
    assert all(x[1] is None for x in texts)
    assert [x[2] for x in texts] == DOCUMENT_NAMES
    assert [len(x[3]) for x in texts] == [188, 636691, 0, 0]

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2))

    assert {x[0] for x in texts} == set(DOCUMENT_NAMES)
    assert all(x[1] is None for x in texts)
    assert {x[2] for x in texts} == set(DOCUMENT_NAMES)
    assert {len(x[3]) for x in texts} == {188, 636691, 0, 0}

    texts = list(
        iterators.ProtocolTextIterator(filenames=filenames, level='protocol', skip_size=0, processes=2, ordered=True)
    )
    assert [x[0] for x in texts] == DOCUMENT_NAMES

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='protocol', skip_size=1, processes=None))

    assert [len(x[3]) for x in texts] == [188, 636691]

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='speech', skip_size=1, processes=None))

    assert len(texts) == 33

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='speaker', skip_size=1, processes=None))

    assert len(texts) == 33

    texts = list(iterators.ProtocolTextIterator(filenames=filenames, level='utterance', skip_size=1, processes=None))

    assert len(texts) == 378

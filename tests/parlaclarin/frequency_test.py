import os
import uuid
from typing import List

import pytest

from pyriksprot import interface, utility, workflows
from pyriksprot.configuration.inject import ConfigStore
from pyriksprot.corpus import parlaclarin
from tests.parlaclarin.utility import get_test_filenames

from .. import fakes

jj = os.path.join

TEST_PARLACLARIN_XML_FILES = [
    ("prot-1933--fk--5.xml", 'protocol', 1),
    ("prot-1955--ak--22.xml", 'protocol', 1),
    ("prot-197879--14.xml", 'protocol', 1),
    ("prot-199596--35.xml", 'protocol', 1),
]


@pytest.mark.parametrize('texts', ["a a b c c d e f a e", ["a a b c c", "d e f a e"]])
def test_word_frequency_counter(texts):
    counter: parlaclarin.TermFrequencyCounter = parlaclarin.TermFrequencyCounter(progress=False)

    counter.ingest(texts)

    assert counter.frequencies.get('a', None) == 3
    assert counter.frequencies.get('b', None) == 1
    assert counter.frequencies.get('c', None) == 2
    assert counter.frequencies.get('d', None) == 1
    assert counter.frequencies.get('e', None) == 2
    assert counter.frequencies.get('f', None) == 1


@pytest.mark.parametrize('filename', get_test_filenames())
def test_word_frequency_counter_ingest_parla_clarin_files(filename: str):
    texts = parlaclarin.XmlProtocolSegmentIterator(filenames=[filename], segment_level='protocol')
    counter: parlaclarin.TermFrequencyCounter = parlaclarin.TermFrequencyCounter(progress=False)
    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(filename)

    counter.ingest(texts)

    assert protocol.has_text == (len(counter.frequencies) > 0)


@pytest.mark.parametrize('filename', get_test_filenames())
def test_persist_word_frequencies(filename: List[str]):
    texts = parlaclarin.XmlProtocolSegmentIterator(filenames=[filename], segment_level='protocol')
    counter: parlaclarin.TermFrequencyCounter = parlaclarin.TermFrequencyCounter(progress=False)

    counter.ingest(texts)

    store_name = jj("tests", "output", "test_persist_word_frequencies.pkl")
    counter.store(store_name)

    assert os.path.isfile(store_name)

    wf = parlaclarin.TermFrequencyCounter.load(store_name)
    assert counter.frequencies == wf

    # os.unlink(store_name)


@pytest.mark.parametrize(
    'document_name,',
    ['prot-1958-fake', 'prot-1960-fake', 'prot-1980-empty'],
)
def test_compute_word_frequencies(document_name: str):
    fakes_folder: str = ConfigStore.config().get("fakes:folder")
    filename: str = jj(fakes_folder, f"{document_name}.xml")
    expected_frequencies: dict[str, int] = fakes.sample_compute_expected_counts(filename, kind='token', lowercase=True)

    target_filename: str = jj("tests", "output", f"{str(uuid.uuid4())[:8]}_word_frequencies.pkl")

    with utility.temporary_file(filename=target_filename) as store_name:
        counts: parlaclarin.TermFrequencyCounter = workflows.compute_term_frequencies(
            source=[filename],
            filename=store_name,
            progress=False,
        )
        assert os.path.isfile(store_name)

    assert dict(counts.frequencies) == expected_frequencies

    with utility.temporary_file(filename=target_filename) as store_name:
        counts: parlaclarin.TermFrequencyCounter = workflows.compute_term_frequencies(
            source=[filename],
            filename=store_name,
            multiproc_processes=2,
            progress=False,
        )
        assert os.path.isfile(store_name)

    assert dict(counts.frequencies) == expected_frequencies

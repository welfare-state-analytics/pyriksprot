import os

from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper

jj = os.path.join

RIKSPROT_REPOSITORY_TAG = os.environ["RIKSPROT_REPOSITORY_TAG"]

def test_collect_utterance_whos():
    corpus_folder: str = jj("./tests/test_data/source", RIKSPROT_REPOSITORY_TAG, "parlaclarin")
    protocols, utterances = md.generate_utterance_index(ProtocolMapper, corpus_folder)
    assert protocols is not None
    assert utterances is not None

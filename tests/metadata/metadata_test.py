import os

import pytest
from loguru import logger

from pyriksprot import metadata as md
from pyriksprot.corpus.parlaclarin import ProtocolMapper

from ..utility import create_subset_metadata_to_folder, ensure_test_corpora_exist

jj = os.path.join

FORCE_RUN_SKIPS = os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_to_folder():
    target_folder: str = './metadata/data/'
    md.download_to_folder(tag="v0.4.0", folder=target_folder, force=True)
    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in md.RIKSPROT_METADATA_TABLES)


def test_collect_utterance_whos():
    corpus_folder: str = "./tests/test_data/source/parlaclarin/v0.4.0"
    protocols, utterances = md.generate_utterance_index(ProtocolMapper, corpus_folder)
    assert protocols is not None
    assert utterances is not None


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist()


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_create_subset_metadata_to_folder():
    logger.info("creating sub-setted test metadata")
    create_subset_metadata_to_folder()

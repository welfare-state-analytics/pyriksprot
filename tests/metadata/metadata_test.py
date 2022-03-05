import os

import pytest

from pyriksprot import metadata as md

from ..utility import PARLACLARIN_SOURCE_FOLDER, TAGGED_METADATA_DATABASE_NAME, ensure_test_corpora_exist

jj = os.path.join


def test_to_folder():

    target_folder: str = './metadata/data/'

    md.download_to_folder(tag="v0.4.0", folder=target_folder, force=True)

    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in md.RIKSPROT_METADATA_TABLES)


def test_collect_utterance_whos():
    corpus_folder: str = "./tests/test_data/source/parlaclarin/v0.4.0"
    protocols, utterances = md.generate_utterance_index(corpus_folder)
    assert protocols is not None
    assert utterances is not None


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist()


@pytest.mark.skip(reason="Test infrastructure test")
def test_subset_to_folder():
    md.subset_to_folder(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        source_metadata="metadata/data",
        target_folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
    )
    md.create_database(
        TAGGED_METADATA_DATABASE_NAME,
        branch=None,
        folder=jj(PARLACLARIN_SOURCE_FOLDER, "metadata"),
        force=True,
    )

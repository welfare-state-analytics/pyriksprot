import os
from posixpath import splitext

import pytest
import requests

from pyriksprot import corpus as pc
from pyriksprot import metadata as md
from pyriksprot.metadata.config import table_url
from pyriksprot.scripts.csv2pgsql import csv2pgsql

from .utility import (
    RIKSPROT_PARLACLARIN_FOLDER,
    RIKSPROT_REPOSITORY_TAG,
    TEST_DOCUMENTS,
    create_test_corpus_and_metadata,
    create_test_speech_corpus,
    create_test_tagged_frames_corpus,
    ensure_test_corpora_exist,
    sample_parlaclarin_corpus_exists,
    sample_tagged_frames_corpus_exists,
)

jj = os.path.join

FORCE_RUN_SKIPS = os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(condition=sample_parlaclarin_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus():
    protocols: list[str] = TEST_DOCUMENTS
    target_folder: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols")
    pc.download_protocols(
        filenames=protocols, target_folder=target_folder, create_subfolder=True, tag=RIKSPROT_REPOSITORY_TAG
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_to_folder():
    target_folder: str = jj('./metadata/data/', RIKSPROT_REPOSITORY_TAG)
    configs: md.MetadataTableConfigs = md.MetadataTableConfigs()
    md.gh_dl_metadata_by_config(configs=configs, tag=RIKSPROT_REPOSITORY_TAG, folder=target_folder, force=True)
    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in configs.tablesnames0)


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_frames_corpus_exists(), reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus():
    create_test_tagged_frames_corpus()


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_create_test_corpus_and_metadata():
    create_test_corpus_and_metadata()


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_sample_speech_corpora():
    create_test_speech_corpus()


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist(force=True)


def test_gh_tables_data():
    tag: str = "v0.6.0"
    items: list[dict] = md.gh_ls("welfare-state-analytics", "riksdagen-corpus", "corpus/metadata", tag)
    infos: dict[str, dict] = {}
    for item in items:
        table, extension = splitext(item.get("name"))
        if not extension.endswith("csv"):
            continue
        url: str = item.get("download_url", table_url(table, tag))
        data: str = requests.get(url, timeout=10).content.decode("utf-8")
        headers: list[str] = data.splitlines()[0].split(sep=',')
        infos['table'] = {'name': table, 'headers': headers, 'content': data}
    return infos

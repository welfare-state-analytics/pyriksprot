import os
from posixpath import splitext

import pytest
import requests

from pyriksprot import corpus as pc
from pyriksprot import metadata as md
from pyriksprot.configuration import ConfigStore
from pyriksprot.metadata.config import table_url

from .utility import (
    create_test_speech_corpus,
    create_test_tagged_frames_corpus,
    ensure_test_corpora_exist,
    load_test_documents,
    sample_parlaclarin_corpus_exists,
    sample_tagged_frames_corpus_exists,
    subset_corpus_and_metadata,
)

jj = os.path.join

FORCE_RUN_SKIPS = False # os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(condition=sample_parlaclarin_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus():
    protocols: list[str] = load_test_documents()
    target_folder: str = ConfigStore.config().get("corpus.folder")
    version: str = ConfigStore.config().get("corpus.version")
    pc.download_protocols(filenames=protocols, target_folder=target_folder, create_subfolder=True, tag=version)


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_to_folder():
    version: str = ConfigStore.config().get("metadata.version")
    target_folder: str = jj('./metadata/data/', version)
    configs: md.MetadataTableConfigs = md.MetadataTableConfigs(version)
    md.gh_dl_metadata_by_config(configs=configs, tag=version, folder=target_folder, force=True)
    assert all(os.path.isfile(jj(target_folder, f"{basename}.csv")) for basename in configs.tablesnames0)


@pytest.mark.skipif(not FORCE_RUN_SKIPS and sample_tagged_frames_corpus_exists(), reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus():
    version: str = ConfigStore.config().get("version")
    data_folder: str = ConfigStore.config().get("data_folder")
    tagged_folder: str = ConfigStore.config().get("tagged_frames.folder")
    riksprot_tagged_folder: str = jj(data_folder, version, 'tagged_frames')
    create_test_tagged_frames_corpus(
        protocols=load_test_documents(),
        source_folder=riksprot_tagged_folder,
        target_folder=tagged_folder,
    )


@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_subset_corpus_and_metadata():
    version: str = ConfigStore.config().get("metadata.version")
    data_folder: str = ConfigStore.config().get("data_folder")
    subset_corpus_and_metadata(tag=version, target_folder=data_folder, documents=load_test_documents())


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_sample_speech_corpora():
    version: str = ConfigStore.config().get("version")
    tagged_folder: str = ConfigStore.config().get("tagged_frames.folder")
    database: str = ConfigStore.config().get("metadata.database")
    create_test_speech_corpus(
        source_folder=tagged_folder,
        tag=version,
        database_name=database,
    )


@pytest.mark.skip(reason="Test infrastructure test")
def test_setup_test_corpora():
    ensure_test_corpora_exist(force=True)


def test_gh_tables_data():
    tag: str = ConfigStore.config().get("metadata.version")
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

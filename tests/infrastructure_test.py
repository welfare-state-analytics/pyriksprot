import os
from posixpath import splitext

import pytest
import requests

from pyriksprot import corpus as pc
from pyriksprot import gitchen as gh
from pyriksprot import metadata as md
from pyriksprot.configuration import ConfigStore
from pyriksprot.configuration.inject import ConfigValue

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

FORCE_RUN_SKIPS = False  # os.environ.get("PYTEST_FORCE_RUN_SKIPS") is not None


@pytest.mark.skipif(condition=sample_parlaclarin_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus():
    protocols: list[str] = load_test_documents()
    target_folder: str = ConfigStore.config().get("corpus.folder")
    version: str = ConfigStore.config().get("corpus.version")
    opts: dict = ConfigStore.config().get("corpus.github")
    pc.download_protocols(filenames=protocols, target_folder=target_folder, create_subfolder=True, tag=version, **opts)


# @pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_to_folder():
    version: str = ConfigStore.config().get("metadata.version")
    target_folder: str = jj('./metadata/data/', version)
    configs: md.MetadataTableConfigs = md.MetadataTableConfigs(version)
    md.gh_dl_metadata_by_config(schema=configs, tag=version, folder=target_folder, force=True)
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
    opts = ConfigStore.config().get("metadata.github")
    subset_corpus_and_metadata(tag=version, target_folder=data_folder, documents=load_test_documents(), **opts)


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
    tag: str = ConfigValue("metadata.version").resolve()
    cfg: dict[str, str] = ConfigValue("metadata.github").resolve() or {}

    user: str = cfg.get('user')
    repository: str = cfg.get('repository')
    path: str = cfg.get('path')

    items: list[dict] = gh.gh_ls(user=user, repository=repository, path=path, tag=tag)

    infos: dict[str, dict] = {}
    for item in items:
        table, extension = splitext(item.get("name"))
        if not extension.endswith("csv"):
            continue
        url: str = gh.gh_download_url(
            user=cfg.get('user'),
            repository=cfg.get('repository'),
            path=cfg.get('path'),
            filename=f"{item.get('name')}.csv",
            tag=tag,
        )
        data: str = requests.get(url, timeout=10).content.decode("utf-8")
        headers: list[str] = data.splitlines()[0].split(sep=',')
        infos['table'] = {'name': table, 'headers': headers, 'content': data}
    return infos

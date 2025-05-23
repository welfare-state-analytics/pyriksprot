from __future__ import annotations

import logging
import os

import dotenv
import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import, # type: ignore
from loguru import logger

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import to_speech
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus import corpus_index as csi
from pyriksprot.corpus import iterate, tagged
from pyriksprot.dispatch import merge as sg
from pyriksprot.dispatch.item import DispatchItem
from pyriksprot.utility import generate_default_config
from pyriksprot.workflows.subset_corpus import load_document_patterns

from .utility import ensure_test_corpora_exist

# pylint: disable=redefined-outer-name

TEST_CONFIG_FILENAME = 'tests/output/config.yml'
TEST_ROOT_FOLDER = 'tests/test_data/source'


def bootstrap_testing():
    os.makedirs('tests/output', exist_ok=True)

    dotenv.load_dotenv('.env')

    assert os.environ.get('CORPUS_VERSION') is not None, "CORPUS_VERSION must be set in .env file"
    assert os.environ.get('METADATA_VERSION') is not None, "METADATA_VERSION must be set in .env file"

    generate_default_config(
        target_filename=TEST_CONFIG_FILENAME,
        root_folder=TEST_ROOT_FOLDER,
        corpus_version=os.environ.get('CORPUS_VERSION'),
        corpus_folder=f"{TEST_ROOT_FOLDER}/{os.environ.get('CORPUS_VERSION')}/riksdagen-records",
        metadata_version=os.environ.get('METADATA_VERSION'),
        stanza_datadir='/data/sparv/models/stanza',
    )

    ConfigStore.configure_context(source=TEST_CONFIG_FILENAME, env_prefix=None)

    try:
        ensure_test_corpora_exist(only_check=True)
    except Exception:
        logger.error(
            "Test corpora not found. Please run `pytest --setup-only` to download the test data."
        )


bootstrap_testing()


@pytest.fixture(scope='session')
def list_of_test_protocols() -> list[str]:
    return load_document_patterns(filename='tests/test_data/test_documents.txt', extension='xml')


@pytest.fixture(scope='session')
def source_index() -> csi.CorpusSourceIndex | None:
    tagged_source_folder: str = ConfigStore.config().get("tagged_frames.folder")
    try:
        return csi.CorpusSourceIndex.load(
            source_folder=tagged_source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
        )
    except FileNotFoundError:
        logger.error(f"tagged source folder {tagged_source_folder} not found")
        return None

@pytest.fixture(scope='session')
def xml_source_index() -> csi.CorpusSourceIndex:
    source_folder: str = ConfigStore.config().get("corpus.folder")
    items: csi.CorpusSourceIndex = csi.CorpusSourceIndex.load(
        source_folder=source_folder,
        source_pattern='**/prot-*-*.xml',
        skip_empty=False,
    )
    return items


@pytest.fixture(scope='session')
def person_index() -> md.PersonIndex:
    sample_metadata_database_name: str = ConfigStore.config().get("metadata.database.options.filename")
    return md.PersonIndex(database_filename=sample_metadata_database_name).load()


@pytest.fixture(scope='session')
def speaker_service(person_index: md.PersonIndex) -> md.SpeakerInfoService:
    sample_metadata_database_name = ConfigStore.config().get("metadata.database.options.filename")
    return md.SpeakerInfoService(database_filename=sample_metadata_database_name, person_index=person_index)


@pytest.fixture(scope='session')
def tagged_speeches(
    source_index: csi.CorpusSourceIndex,
    speaker_service: md.SpeakerInfoService,
) -> list[dict[str, DispatchItem]]:
    def assign_speaker_info(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id, year=item.year)

    segments: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=to_speech.MergeStrategyType.chain,
        segment_skip_size=1,
        preprocess=assign_speaker_info,
    )
    groups: list[dict[str, DispatchItem]] = sg.SegmentMerger(
        source_index=source_index, temporal_key=None, grouping_keys=None
    ).merge(segments)
    groups = list(groups)
    return groups


@pytest.fixture(scope='session')
def caplog(_caplog):
    class PropogateHandler(logging.Handler):
        def emit(self, record):
            logging.getLogger(record.name).handle(record)

    handler_id = logger.add(PropogateHandler(), format="{message} {extra}")
    yield _caplog
    logger.remove(handler_id)


# # kudos: https://stackoverflow.com/a/61503247/12383895
# def pytest_addoption(parser):
#     parser.addoption("--no-skips", action="store_true", default=False, help="disable skip marks")


# @pytest.hookimpl(tryfirst=True)
# def pytest_cmdline_preparse(config, args):  # pylint: disable=unused-argument
#     if "--no-skips" not in args:
#         return

#     def no_skip(*args, **kwargs):  # pylint: disable=unused-argument
#         return

#     _pytest.skipping.skip = no_skip

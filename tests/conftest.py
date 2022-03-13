from __future__ import annotations

import logging

import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import
from loguru import logger

from pyriksprot.corpus import corpus_index as csi
from pyriksprot import metadata as md

from .utility import (
    PARLACLARIN_SOURCE_FOLDER,
    TAGGED_METADATA_DATABASE_NAME,
    TAGGED_SOURCE_FOLDER,
    ensure_test_corpora_exist,
)

ensure_test_corpora_exist()


@pytest.fixture
def source_index() -> csi.CorpusSourceIndex:
    return csi.CorpusSourceIndex.load(
        source_folder=TAGGED_SOURCE_FOLDER, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )


@pytest.fixture
def xml_source_index() -> csi.CorpusSourceIndex:

    items: csi.CorpusSourceIndex = csi.CorpusSourceIndex.load(
        source_folder=PARLACLARIN_SOURCE_FOLDER,
        source_pattern='**/prot-*.xml',
        skip_empty=False,
    )
    return items


@pytest.fixture
def person_index() -> md.PersonIndex:
    return md.PersonIndex(database_filename=TAGGED_METADATA_DATABASE_NAME).load()


@pytest.fixture
def speaker_service(person_index: md.PersonIndex) -> md.SpeakerInfoService:  # pylint: disable=redefined-outer-name
    return md.SpeakerInfoService(database_filename=TAGGED_METADATA_DATABASE_NAME, person_index=person_index)


@pytest.fixture
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

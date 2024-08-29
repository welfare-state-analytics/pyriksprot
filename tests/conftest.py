from __future__ import annotations

import logging

import pytest
from _pytest.logging import caplog as _caplog  # pylint: disable=unused-import
from loguru import logger

from pyriksprot import interface
from pyriksprot import metadata as md
from pyriksprot import to_speech
from pyriksprot.configuration import ConfigStore
from pyriksprot.corpus import corpus_index as csi
from pyriksprot.corpus import iterate, tagged
from pyriksprot.dispatch import merge as sg

from .utility import ensure_test_corpora_exist

ConfigStore.configure_context(source='tests/config.yml', env_prefix=None)

# pylint: disable=redefined-outer-name

# ensure_test_corpora_exist()


@pytest.fixture(scope='session')
def source_index() -> csi.CorpusSourceIndex:
    tagged_source_folder = ConfigStore.config().get("tagged_frames.folder")
    return csi.CorpusSourceIndex.load(
        source_folder=tagged_source_folder, source_pattern='**/prot-*.zip', years=None, skip_empty=True
    )


@pytest.fixture(scope='session')
def xml_source_index() -> csi.CorpusSourceIndex:
    source_folder = ConfigStore.config().get("corpus.folder")
    items: csi.CorpusSourceIndex = csi.CorpusSourceIndex.load(
        source_folder=source_folder,
        source_pattern='**/prot-*.xml',
        skip_empty=False,
    )
    return items


@pytest.fixture(scope='session')
def person_index() -> md.PersonIndex:
    sample_metadata_database_name = ConfigStore.config().get("metadata.database")
    return md.PersonIndex(database_filename=sample_metadata_database_name).load()


@pytest.fixture(scope='session')
def speaker_service(person_index: md.PersonIndex) -> md.SpeakerInfoService:
    sample_metadata_database_name = ConfigStore.config().get("metadata.database")
    return md.SpeakerInfoService(database_filename=sample_metadata_database_name, person_index=person_index)


@pytest.fixture(scope='session')
def tagged_speeches(
    source_index: csi.CorpusSourceIndex,
    speaker_service: md.SpeakerInfoService,
) -> list[sg.DispatchItem]:
    def assign_speaker_info(item: iterate.ProtocolSegment) -> None:
        item.speaker_info = speaker_service.get_speaker_info(u_id=item.u_id)

    segments: iterate.ProtocolSegmentIterator = tagged.ProtocolIterator(
        filenames=source_index.paths,
        content_type=interface.ContentType.TaggedFrame,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=to_speech.MergeStrategyType.who_speaker_note_id_sequence,
        segment_skip_size=1,
        preprocess=assign_speaker_info,
    )
    groups = sg.SegmentMerger(source_index=source_index, temporal_key=None, grouping_keys=None).merge(segments)
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

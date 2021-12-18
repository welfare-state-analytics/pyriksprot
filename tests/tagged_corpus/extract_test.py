import os
import uuid
from typing import Iterable, List

import pytest

from pyriksprot import CorpusSourceIndex, interface, tagged_corpus

from ..utility import TAGGED_SOURCE_FOLDER

# pylint: disable=redefined-outer-name


DEFAULT_OPTS = dict(
    source_folder=TAGGED_SOURCE_FOLDER,
    target_type='files-in-zip',
    content_type=interface.ContentType.TaggedFrame,
    segment_level=interface.SegmentLevel.Who,
    multiproc_keep_order=None,
    multiproc_processes=1,
    years=None,
    segment_skip_size=1,
)


def test_glob_protocols():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    filenames: List[str] = tagged_corpus.glob_protocols(corpus_source, file_pattern='prot-*.zip', strip_path=True)
    assert len(filenames) == 6
    """Empty files should be included"""
    assert 'prot-1955--ak--22.zip' in filenames


def test_create_source_index_for_tagged_corpus():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    source_index = CorpusSourceIndex.load(source_folder=corpus_source, source_pattern='**/prot-*.zip')
    assert isinstance(source_index, CorpusSourceIndex)
    assert len(source_index) == 5

    source_index = CorpusSourceIndex.load(source_folder=corpus_source, source_pattern='**/prot-*.zip', skip_empty=False)
    assert len(source_index) == 6


def test_load_protocols():
    corpus_source: str = TAGGED_SOURCE_FOLDER
    filenames: List[str] = tagged_corpus.glob_protocols(corpus_source, file_pattern='prot-*.zip')

    protocol_iter: Iterable[interface.Protocol] = tagged_corpus.load_protocols(corpus_source)
    protocols = list(protocol_iter)

    """Empty files should NOT be included"""
    assert len(protocols) == len(filenames) - 1


@pytest.mark.parametrize(
    'temporal_key, group_keys',
    [
        (interface.TemporalKey.Year, [interface.GroupingKey.Party]),
        (interface.TemporalKey.Year, []),
        (interface.TemporalKey.Protocol, []),
        (None, []),
    ],
)
def test_extract_corpus_tags_with_various_groupings(temporal_key, group_keys):

    target_name = f'tests/output/{temporal_key}_{"_".join(group_keys)}_{uuid.uuid1()}.zip'
    opts = {
        **DEFAULT_OPTS,
        **dict(
            target_name=target_name,
            temporal_key=temporal_key,
            group_keys=group_keys,
        ),
    }

    tagged_corpus.extract_corpus_tags(**opts, progress=False)
    assert os.path.isfile(opts['target_name'])
    os.unlink(opts['target_name'])

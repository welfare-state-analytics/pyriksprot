import os
import pickle
from unittest.mock import Mock, patch

import pytest

from pyriksprot import temporary_file
from pyriksprot.dehyphenation.swe_dehyphen import (
    ParagraphMergeStrategy,
    SwedishDehyphenatorService,
    find_dashed_words,
    merge_paragraphs,
)

jj = os.path.join
nj = os.path.normpath

# pylint: disable=redefined-outer-name

os.makedirs(jj("tests", "output"), exist_ok=True)


@pytest.fixture
def cfg() -> dict:
    s
    return dict(
        word_frequency_filename='./tests/output/riksdagen-corpus-term-frequencies.pkl',
        whitelist_filename='./tests/output/output/dehyphen_whitelist.txt.gz',
        whitelist_log_filename='./tests/output/output/dehyphen_whitelist_log.pkl',
        unresolved_filename='./tests/output/dehyphen_unresolved.txt.gz',
    )


def test_merge_paragraphs():

    text = "Detta är en \n\nmening"
    result = merge_paragraphs(text, ParagraphMergeStrategy.DoNotMerge)
    assert result == text


# @pytest.mark.slow
# def test_dehyphen_service():

#     service = FlairDehyphenService(lang="sv")

#     expected_text = "Detta är en\nenkel text.\n\nDen har tre paragrafer.\n\nDetta är den tredje paragrafen."
#     dehyphened_text = service.dehyphen_text(expected_text, merge_paragraphs=False)
#     assert dehyphened_text == expected_text

#     text = "Detta är en\nenkel text.\n\nDen har tre paragrafer.\n   \t\n\n   \nDetta är den tredje paragrafen."
#     dehyphened_text = service.dehyphen_text(text, merge_paragraphs=False)
#     assert dehyphened_text == expected_text


def test_create_dehyphenator_service_fails_if_no_word_frequency_file(cfg):

    if os.path.isfile(cfg.get('word_frequency_filename')):
        os.remove(cfg.get('word_frequency_filename'))

    with pytest.raises(FileNotFoundError):
        with patch('pyriksprot.dehyphenation.swe_dehyphen.SwedishDehyphenator', return_value=Mock()) as _:
            _ = SwedishDehyphenatorService(**cfg)


def test_create_dehyphenator_service_succeeds_when_frequency_file_exists(cfg):
    with temporary_file(filename=cfg.get('word_frequency_filename'), content=pickle.dumps({'a': 1})):
        with patch(
            'pyriksprot.dehyphenation.swe_dehyphen.SwedishDehyphenator', return_value=Mock()
        ) as mock_dehyphenator:
            _ = SwedishDehyphenatorService(**cfg)
            mock_dehyphenator.assert_called_once()


def test_dehyphenator_service_flush_creates_expected_files(cfg):
    with temporary_file(filename=cfg.get('word_frequency_filename'), content=pickle.dumps({'a': 1})):

        service: SwedishDehyphenatorService = SwedishDehyphenatorService(**cfg)

        service.flush()

        assert os.path.isfile(service.whitelist_filename)
        assert os.path.isfile(service.unresolved_filename)
        assert os.path.isfile(service.whitelist_log_filename)

        os.remove(service.whitelist_filename)
        os.remove(service.unresolved_filename)
        os.remove(service.whitelist_log_filename)


def test_dehyphenator_service_can_load_flushed_data(cfg):

    with temporary_file(filename=cfg.get('word_frequency_filename'), content=pickle.dumps({'a': 1})):

        service: SwedishDehyphenatorService = SwedishDehyphenatorService(**cfg)

        service.dehyphenator.unresolved = {"a", "b", "c"}
        service.dehyphenator.whitelist = {"e", "f", "g"}
        service.dehyphenator.whitelist_log = {"e": 0, "f": 1, "g": 1}

        service.flush()

        assert os.path.isfile(service.whitelist_filename)
        assert os.path.isfile(service.unresolved_filename)
        assert os.path.isfile(service.whitelist_log_filename)

        service2 = SwedishDehyphenatorService(**cfg)

        assert service2.dehyphenator.whitelist == service.dehyphenator.whitelist
        assert service2.dehyphenator.unresolved == service.dehyphenator.unresolved
        assert service2.dehyphenator.whitelist_log == service.dehyphenator.whitelist_log

        os.remove(service.whitelist_filename)
        os.remove(service.unresolved_filename)
        os.remove(service.whitelist_log_filename)


def test_find_dashed_words():
    text = "Detta mening har inget binde- streck. Eva-Marie är ett namn. IKEA-möbler. 10-tal. "
    tokens = find_dashed_words(text)
    assert tokens is not None


def test_dehyphenator_service_dehyphen(cfg):

    dehyphenator = SwedishDehyphenatorService(
        **cfg,
        word_frequencies={'a': 1},
        whitelist=set(),
        unresolved=set(),
        whitelist_log={},
    ).dehyphenator

    text = "Detta mening har inget bindestreck."
    result = dehyphenator.dehyphen_text(text)
    assert result == text
    assert len(dehyphenator.whitelist) == 0
    assert len(dehyphenator.unresolved) == 0

    text = "Detta mening har inget binde-streck."
    result = dehyphenator.dehyphen_text(text)
    assert result == text
    assert len(dehyphenator.whitelist) == 0
    assert len(dehyphenator.unresolved) == 0

    text = "Detta mening har ett binde-\nstreck. Eva-Marie är ett namn. IKEA-\nmöbler. 10-\n\ntal. "
    dehyphenator.word_frequencies = {'bindestreck': 2, 'binde-streck': 1}
    result = dehyphenator.dehyphen_text(text)
    assert result == "Detta mening har ett bindestreck. Eva-Marie är ett namn. IKEA-möbler. 10-tal."
    assert dehyphenator.whitelist == {'bindestreck', 'ikea-möbler', '10-tal'}
    assert len(dehyphenator.unresolved) == 0


def test_dehyphenator_service_dehyphen_by_frequency(cfg):

    dehyphenator = SwedishDehyphenatorService(
        **cfg,
        word_frequencies={'a': 1},
        whitelist=set(),
        unresolved=set(),
        whitelist_log={},
    ).dehyphenator

    text = "Detta är ett binde-\nstreck. "
    dehyphenator.word_frequencies = {'bindestreck': 1, 'binde-streck': 2}
    result = dehyphenator.dehyphen_text(text)
    assert result == "Detta är ett binde-streck."
    assert dehyphenator.whitelist == {'binde-streck'}
    assert len(dehyphenator.unresolved) == 0

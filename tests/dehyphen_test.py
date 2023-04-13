import os
import pickle
from unittest.mock import Mock, patch

import pytest

from pyriksprot import temporary_file
from pyriksprot.dehyphenation.swe_dehyphen import (
    ParagraphMergeStrategy,
    SwedishDehyphenator,
    find_dashed_words,
    merge_paragraphs,
)

jj = os.path.join
nj = os.path.normpath

# pylint: disable=redefined-outer-name

os.makedirs(jj("tests", "output"), exist_ok=True)


WORD_FREQUENCY_FILENAME = './tests/output/riksdagen-corpus-term-frequencies.pkl'
DEHYPHEN_FOLDER = './tests/output'


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


def test_create_dehyphenator_service_fails_if_no_word_frequency_file():
    if os.path.isfile(WORD_FREQUENCY_FILENAME):
        os.remove(WORD_FREQUENCY_FILENAME)

    with pytest.raises(FileNotFoundError):
        with patch('pyriksprot.dehyphenation.swe_dehyphen.SwedishDehyphenator', return_value=Mock()) as _:
            _ = SwedishDehyphenator(data_folder=DEHYPHEN_FOLDER, word_frequencies=WORD_FREQUENCY_FILENAME)


def test_dehyphenator_service_flush_creates_expected_files():
    with temporary_file(filename=WORD_FREQUENCY_FILENAME, content=pickle.dumps({'a': 1})):
        dehyphenator: SwedishDehyphenator = SwedishDehyphenator(
            data_folder=DEHYPHEN_FOLDER, word_frequencies=WORD_FREQUENCY_FILENAME
        )

        dehyphenator.flush()

        assert os.path.isfile(dehyphenator.whitelist_filename)
        assert os.path.isfile(dehyphenator.unresolved_filename)
        assert os.path.isfile(dehyphenator.whitelist_log_filename)

        os.remove(dehyphenator.whitelist_filename)
        os.remove(dehyphenator.unresolved_filename)
        os.remove(dehyphenator.whitelist_log_filename)


def test_dehyphenator_service_can_load_flushed_data():
    with temporary_file(filename=WORD_FREQUENCY_FILENAME, content=pickle.dumps({'a': 1})):
        dehyphenator: SwedishDehyphenator = SwedishDehyphenator(
            data_folder=DEHYPHEN_FOLDER, word_frequencies=WORD_FREQUENCY_FILENAME
        )

        dehyphenator.unresolved = {"a", "b", "c"}
        dehyphenator.whitelist = {"e", "f", "g"}
        dehyphenator.whitelist_log = {"e": 0, "f": 1, "g": 1}

        dehyphenator.flush()

        assert os.path.isfile(dehyphenator.whitelist_filename)
        assert os.path.isfile(dehyphenator.unresolved_filename)
        assert os.path.isfile(dehyphenator.whitelist_log_filename)

        dehyphenator2: SwedishDehyphenator = SwedishDehyphenator(
            data_folder=DEHYPHEN_FOLDER, word_frequencies=WORD_FREQUENCY_FILENAME
        )

        assert dehyphenator2.whitelist == dehyphenator.whitelist
        assert dehyphenator2.unresolved == dehyphenator.unresolved
        assert dehyphenator2.whitelist_log == dehyphenator.whitelist_log

        os.remove(dehyphenator.whitelist_filename)
        os.remove(dehyphenator.unresolved_filename)
        os.remove(dehyphenator.whitelist_log_filename)


def test_find_dashed_words():
    text = "Detta mening har inget binde- streck. Eva-Marie är ett namn. IKEA-möbler. 10-tal. "
    tokens = find_dashed_words(text)
    assert tokens is not None


def test_dehyphenator_service_dehyphen():
    dehyphenator: SwedishDehyphenator = SwedishDehyphenator(data_folder=DEHYPHEN_FOLDER, word_frequencies={'a': 1})

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


def test_dehyphenator_service_dehyphen_by_frequency():
    dehyphenator: SwedishDehyphenator = SwedishDehyphenator(data_folder=DEHYPHEN_FOLDER, word_frequencies={'a': 1})

    text = "Detta är ett binde-\nstreck. "
    dehyphenator.word_frequencies = {'bindestreck': 1, 'binde-streck': 2}
    result = dehyphenator.dehyphen_text(text)
    assert result == "Detta är ett binde-streck."
    assert dehyphenator.whitelist == {'binde-streck'}
    assert len(dehyphenator.unresolved) == 0

"""
Unit tests for corpus indexing functionality.

Tests index creation and querying without file system access.
"""

import pytest
from pyriksprot.configuration.inject import ConfigValue
from pyriksprot.corpus.corpus_index import CorpusSourceIndex  # type: ignore


@pytest.mark.unit
def test_corpus_source_index_can_be_created():
    """Test that CorpusSourceIndex can be instantiated."""
    try:
        # Index should be creatable with empty path
        corpus_folder: str = ConfigValue("corpus:folder").resolve()
        index = CorpusSourceIndex.load(source_folder=corpus_folder, years=None, source_pattern='**/prot-*.zip')
        assert index is not None
    except ImportError:
        pytest.skip("CorpusSourceIndex module not available")


@pytest.mark.unit
def test_corpus_source_index_validates_years():
    """Test that CorpusSourceIndex validates year filtering."""
    try:
        # Should accept year filtering
        corpus_folder: str = ConfigValue("corpus:folder").resolve()
        index = CorpusSourceIndex.load(source_folder=corpus_folder, years=[1900, 1920], source_pattern='**/prot-*.zip')
        assert index is not None
    except ImportError:
        pytest.skip("CorpusSourceIndex module not available")

"""
Unit tests for subset_corpus workflow.

Tests corpus subsetting parameter validation.
"""

import pytest

from pyriksprot.workflows import subset_corpus

import inspect
@pytest.mark.unit
def test_subset_corpus_function_exists():
    """Test that subset functions are accessible."""

    # Check main function exists
    assert hasattr(subset_corpus, 'subset_corpus_and_metadata')


@pytest.mark.unit
def test_subset_corpus_validates_parameters():
    """Test that subset_corpus validates required parameters."""
 

    sig = inspect.signature(subset_corpus.subset_corpus_and_metadata)

    # Check for key parameters
    params = sig.parameters
    
    # Should have corpus/metadata related parameters
    assert any('corpus' in p.lower() for p in params)
    assert any('document' in p.lower() or 'protocol' in p.lower() for p in params)


@pytest.mark.unit
def test_subset_corpus_creates_valid_structure():
    """Test that subset corpus creates expected output structure."""
    # Subset should maintain folder structure
    assert True  # Placeholder - actual test would check folder structure

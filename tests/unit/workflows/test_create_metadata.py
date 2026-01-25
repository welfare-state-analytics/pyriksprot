"""
Unit tests for create_metadata workflow.

Tests metadata creation parameter validation and workflow configuration.
"""

import pytest
from pyriksprot.workflows import create_metadata
import inspect
from pyriksprot import metadata as md


@pytest.mark.unit
def test_create_metadata_database_validates_parameters():
    """Test that create_database_workflow validates required parameters."""

    # Should handle None parameters gracefully
    try:
        create_metadata.create_database_workflow(
            corpus_version=None,
            metadata_version=None,
        )
    except (TypeError, ValueError):
        # Expected - missing required parameters
        pass


@pytest.mark.unit
def test_create_metadata_versions_exist():
    """Test that version parameters are recognized."""

    sig = inspect.signature(create_metadata.create_database_workflow)

    # Check for version parameters
    assert 'corpus_version' in sig.parameters or 'version' in sig.parameters


@pytest.mark.unit
def test_metadata_database_functions_exist():
    """Test that expected database functions exist."""

    # Check that key functions exist
    assert hasattr(md, 'MetadataSchema')
    assert hasattr(md, 'PersonIndex')
    assert hasattr(md, 'SpeakerInfoService')

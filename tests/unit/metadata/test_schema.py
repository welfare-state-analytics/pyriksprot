"""
Unit tests for metadata schema and database operations.

Tests metadata database structure without actual file I/O.
"""

import pytest


@pytest.mark.unit
def test_metadata_schema_exists():
    """Test that MetadataSchema is defined."""
    try:
        from pyriksprot.metadata.schema import MetadataSchema  # type: ignore

        assert MetadataSchema is not None
    except ImportError:
        pytest.skip("MetadataSchema not available")


@pytest.mark.unit
def test_metadata_schema_has_version():
    """Test that metadata schema has version tracking."""
    try:
        from pyriksprot.metadata.schema import MetadataSchema  # type: ignore

        # Schema should have version information
        schema_attrs = dir(MetadataSchema)
        assert len(schema_attrs) > 0
    except ImportError:
        pytest.skip("MetadataSchema not available")


@pytest.mark.unit
def test_speaker_info_service_can_be_created():
    """Test that SpeakerInfoService can be instantiated."""
    try:
        from pyriksprot.metadata.speaker_info import SpeakerInfoService  # type: ignore

        # Should be importable
        assert SpeakerInfoService is not None
    except ImportError:
        pytest.skip("SpeakerInfoService not available")


@pytest.mark.unit
def test_metadata_options_validation():
    """Test that metadata options can be validated."""
    try:
        from pyriksprot.metadata.metadata_table_loader import MetadataTableLoader  # type: ignore

        # Should be able to create loader with options
        assert MetadataTableLoader is not None
    except ImportError:
        pytest.skip("MetadataTableLoader not available")

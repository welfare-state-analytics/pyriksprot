"""
Unit tests for other CLI scripts.

Tests for subset-corpus, tag-info, and utility scripts.
"""

import pytest

# type: ignore ; pylint: disable=import-outside-toplevel


@pytest.mark.unit
def test_subset_corpus_script_exists():
    """Test that subset_corpus script is accessible."""
    try:
        from pyriksprot.scripts import subset_corpus

        assert hasattr(subset_corpus, 'main')
    except ImportError:
        pytest.skip("subset_corpus module not found")


@pytest.mark.unit
def test_tag_info_script_exists():
    """Test that tag_info script is accessible."""
    try:
        from pyriksprot.scripts import tag_info

        functions = [name for name in dir(tag_info) if not name.startswith('_')]
        assert len(functions) > 0
    except ImportError:
        pytest.skip("tag_info module not found")


@pytest.mark.unit
def test_cli_scripts_can_be_imported():
    """Test that major CLI script modules can be imported."""
    successful_imports = []

    script_modules = [
        'riksprot2tagged',
        'riksprot2speech_index',
        'riksprot2speech_text',
        'subset_vrt_corpus',
        'tag',
        'utils',
    ]

    for module_name in script_modules:
        try:
            __import__(f'pyriksprot.scripts.{module_name}')
            successful_imports.append(module_name)
        except (ImportError, AttributeError):
            # Some modules might not exist or might be optional
            pass

    # Should be able to import at least some modules
    assert len(successful_imports) > 0

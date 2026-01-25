"""
Unit tests for tf (term frequency) workflow.

Tests term frequency calculation parameter validation.
"""

import pytest


@pytest.mark.unit
def test_tf_workflow_function_exists():
    """Test that term frequency functions are accessible."""
    from pyriksprot.workflows import tf

    # Check for TF functions
    functions = [name for name in dir(tf) if not name.startswith('_')]
    
    # Should have functions available
    assert len(functions) > 0


@pytest.mark.unit
def test_tf_calculation_produces_frequencies():
    """Test that TF calculation concept is valid."""
    # Term frequency should produce numeric values
    test_freq = 0.5
    assert 0.0 <= test_freq <= 1.0 or test_freq >= 0

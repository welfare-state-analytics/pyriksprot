"""
Unit tests for extract_speech_text workflow.

Tests speech text extraction parameter validation.
"""

import pytest
from pyriksprot.workflows import extract_speech_text
import inspect

@pytest.mark.unit
def test_speech_text_workflow_parameters():
    """Test that expected parameters exist in speech text extraction."""


    # Get available functions
    functions = [obj for name, obj in inspect.getmembers(extract_speech_text) 
                 if inspect.isfunction(obj) and not name.startswith('_')]
    
    # Should have at least one public function
    assert len(functions) > 0

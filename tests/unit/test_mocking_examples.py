"""
Unit tests demonstrating mocking patterns.

This file shows best practices for mocking file I/O, databases,
configuration, and external services in unit tests.
"""

import pytest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path


@pytest.mark.unit
class TestFileMocking:
    """Examples of mocking file system operations."""

    def test_read_file_with_mock_content(self):
        """Mock file reading."""
        fake_content = "line1\nline2\nline3"
        
        with patch("builtins.open", mock_open(read_data=fake_content)):
            with open("dummy.txt") as f:
                content = f.read()
        
        assert content == fake_content
        assert "line1" in content

    def test_file_not_found_error(self):
        """Mock file not found error."""
        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                with open("missing.txt") as f:
                    f.read()

    def test_check_file_exists(self):
        """Mock Path.exists()."""
        with patch("pathlib.Path.exists", return_value=True):
            assert Path("dummy.txt").exists()
        
        with patch("pathlib.Path.exists", return_value=False):
            assert not Path("missing.txt").exists()

    def test_create_directory(self):
        """Mock directory creation."""
        with patch("pathlib.Path.mkdir") as mock_mkdir:
            Path("/tmp/output").mkdir(parents=True, exist_ok=True)
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_write_to_file(self):
        """Mock file writing."""
        mock_file = mock_open()
        
        with patch("builtins.open", mock_file):
            with open("output.txt", "w") as f:
                f.write("test data")
        
        mock_file.assert_called_once_with("output.txt", "w")
        handle = mock_file()
        handle.write.assert_called_with("test data")


@pytest.mark.unit
class TestConfigurationMocking:
    """Examples of mocking configuration operations."""

    def test_config_value_resolution(self):
        """Mock ConfigValue.resolve()."""
        with patch("pyriksprot.configuration.ConfigValue.resolve") as mock_resolve:
            mock_resolve.return_value = "/path/to/corpus"
            
            # Now ConfigValue resolves to mocked value
            value = mock_resolve()
            assert value == "/path/to/corpus"

    def test_environment_variable_override(self):
        """Mock environment variable."""
        with patch.dict("os.environ", {"PYRIKSPROT_CORPUS_VERSION": "v1.4.1"}):
            import os
            version = os.environ.get("PYRIKSPROT_CORPUS_VERSION")
            assert version == "v1.4.1"

    def test_missing_environment_variable(self):
        """Mock missing environment variable."""
        with patch.dict("os.environ", {}, clear=True):
            import os
            with pytest.raises(KeyError):
                _ = os.environ["NONEXISTENT_VAR"]


@pytest.mark.unit
class TestDatabaseMocking:
    """Examples of mocking database operations."""

    def test_database_connection(self):
        """Mock SQLite connection."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (1, "John Doe"),
            (2, "Jane Smith"),
        ]
        
        with patch("sqlite3.connect", return_value=mock_connection):
            # Simulate code that uses database
            import sqlite3
            conn = sqlite3.connect("test.db")
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM persons")
            results = cursor.fetchall()
        
        assert len(results) == 2
        assert results[0] == (1, "John Doe")

    def test_database_connection_error(self):
        """Mock database connection failure."""
        with patch("sqlite3.connect", side_effect=Exception("Connection failed")):
            import sqlite3
            with pytest.raises(Exception):
                sqlite3.connect("bad.db")

    def test_database_query_results(self):
        """Mock database query with specific results."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Configure return value for a specific query
        mock_cursor.execute.return_value.fetchone.return_value = {
            "person_id": 123,
            "name": "Speaker",
            "party": "S",
        }
        
        with patch("sqlite3.connect", return_value=mock_connection):
            import sqlite3
            conn = sqlite3.connect("test.db")
            cursor = conn.cursor()
            result = cursor.execute("SELECT * FROM persons WHERE id=?", (123,)).fetchone()
        
        assert result["name"] == "Speaker"


@pytest.mark.unit
class TestServiceMocking:
    """Examples of mocking external services."""

    def test_speaker_service_mock(self):
        """Mock SpeakerInfoService."""
        from unittest.mock import MagicMock
        
        mock_service = MagicMock()
        mock_service.get_speaker_info.return_value = {
            "person_id": "p-123",
            "name": "Speaker Name",
            "party": "S",
        }
        
        # Use the mocked service
        result = mock_service.get_speaker_info("u-1")
        
        assert result["name"] == "Speaker Name"
        mock_service.get_speaker_info.assert_called_once_with("u-1")

    def test_service_error_handling(self):
        """Mock service that raises error."""
        mock_service = MagicMock()
        mock_service.get_speaker_info.side_effect = ValueError("Speaker not found")
        
        with pytest.raises(ValueError):
            mock_service.get_speaker_info("unknown-id")
    
    def test_service_multiple_calls(self):
        """Mock service with multiple calls."""
        mock_service = MagicMock()
        mock_service.get_data.side_effect = ["data1", "data2", "data3"]
        
        assert mock_service.get_data() == "data1"
        assert mock_service.get_data() == "data2"
        assert mock_service.get_data() == "data3"
        
        assert mock_service.get_data.call_count == 3


@pytest.mark.unit
class TestGlobMocking:
    """Examples of mocking glob file searches."""

    def test_find_files_with_glob(self):
        """Mock glob.glob() for file finding."""
        mock_files = [
            "prot-1950--001.xml",
            "prot-1950--002.xml",
            "prot-1951--001.xml",
        ]
        
        with patch("glob.glob", return_value=mock_files):
            import glob
            files = glob.glob("*.xml")
        
        assert len(files) == 3
        assert "prot-1950--001.xml" in files

    def test_no_files_found(self):
        """Mock glob returning empty list."""
        with patch("glob.glob", return_value=[]):
            import glob
            files = glob.glob("*.nonexistent")
        
        assert files == []


@pytest.mark.unit
class TestMockAssertions:
    """Examples of asserting mock behavior."""

    def test_assert_called_once(self):
        """Verify function was called exactly once."""
        mock_func = MagicMock()
        
        mock_func(1, 2, 3)
        
        mock_func.assert_called_once_with(1, 2, 3)

    def test_assert_call_count(self):
        """Verify function was called specific number of times."""
        mock_func = MagicMock()
        
        mock_func("a")
        mock_func("b")
        mock_func("c")
        
        assert mock_func.call_count == 3

    def test_assert_not_called(self):
        """Verify function was not called."""
        mock_func = MagicMock()
        
        mock_func.assert_not_called()

    def test_verify_call_arguments(self):
        """Verify function was called with specific arguments."""
        mock_func = MagicMock()
        
        mock_func("arg1", "arg2", keyword="value")
        
        # Check it was called with these args
        mock_func.assert_called_with("arg1", "arg2", keyword="value")


@pytest.mark.unit
class TestContextManagerMocking:
    """Examples using context managers for mocking."""

    def test_with_multiple_patches(self):
        """Mock multiple functions in same test."""
        with patch("builtins.open") as mock_open_file, \
             patch("pathlib.Path.exists") as mock_exists:
            
            mock_open_file.return_value.__enter__.return_value.read.return_value = "content"
            mock_exists.return_value = True
            
            # Test code that uses both operations
            path = Path("file.txt")
            if path.exists():
                with open(path) as f:
                    content = f.read()
            
            assert content == "content"
            mock_exists.assert_called()
            mock_open_file.assert_called()

    def test_with_nested_patches(self):
        """Mock function that calls another mocked function."""
        with patch("builtins.open") as mock_external:
            with patch("builtins.print", return_value=42):
                mock_external.return_value = 100
                
                result = mock_external()
                assert result == 100

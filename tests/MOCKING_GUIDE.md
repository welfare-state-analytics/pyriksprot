# Mocking and Patching Guide

This guide provides comprehensive examples of mocking patterns used in the Pyriksprot test suite.

## Table of Contents

1. [File System Mocking](#file-system-mocking)
2. [Database Mocking](#database-mocking)
3. [Configuration Mocking](#configuration-mocking)
4. [Service Mocking](#service-mocking)
5. [Time and Random Mocking](#time-and-random-mocking)
6. [Multiprocessing Mocking](#multiprocessing-mocking)
7. [Best Practices](#best-practices)

---

## File System Mocking

### Mock File Reading

```python
from unittest.mock import patch, mock_open
import pytest

@pytest.mark.unit
def test_load_xml_file():
    """Mock file reading."""
    xml_content = '<?xml version="1.0"?><root></root>'
    
    with patch('builtins.open', mock_open(read_data=xml_content)):
        content = read_file('dummy.xml')
        assert '<root>' in content
```

### Mock File Writing

```python
@pytest.mark.unit
def test_save_output():
    """Mock file writing."""
    mock_file = mock_open()
    
    with patch('builtins.open', mock_file):
        save_output('data', 'output.txt')
    
    # Verify file was opened for writing
    mock_file.assert_called_once_with('output.txt', 'w')
    # Verify data was written
    handle = mock_file()
    handle.write.assert_called_with('data')
```

### Mock Directory Operations

```python
from pathlib import Path

@pytest.mark.unit
def test_create_output_directory():
    """Mock directory creation."""
    with patch('pathlib.Path.mkdir') as mock_mkdir:
        create_directory(Path('/tmp/output'))
        mock_mkdir.assert_called_once()

@pytest.mark.unit
def test_check_file_exists():
    """Mock file existence check."""
    with patch('pathlib.Path.exists', return_value=True):
        assert file_exists('/tmp/file.txt')
    
    with patch('pathlib.Path.exists', return_value=False):
        assert not file_exists('/tmp/missing.txt')
```

### Mock Glob Operations

```python
@pytest.mark.unit
def test_find_protocols():
    """Mock glob file search."""
    mock_files = [
        'prot-1950--001.xml',
        'prot-1950--002.xml',
        'prot-1951--001.xml',
    ]
    
    with patch('glob.glob', return_value=mock_files):
        files = find_protocols('*.xml')
        assert len(files) == 3
```

---

## Database Mocking

### Mock SQLite Connection

```python
from unittest.mock import MagicMock, patch
import sqlite3

@pytest.mark.unit
def test_person_index_initialization():
    """Mock SQLite connection."""
    mock_cursor = MagicMock()
    mock_cursor.execute.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [
        (1, 'John Doe', 'S'),
        (2, 'Jane Smith', 'M'),
    ]
    
    mock_db = MagicMock()
    mock_db.cursor.return_value = mock_cursor
    
    with patch('sqlite3.connect', return_value=mock_db):
        index = PersonIndex('dummy.db')
        people = index.all()
        assert len(people) == 2
```

### Mock Database Query Results

```python
@pytest.mark.unit
def test_get_speaker_by_id():
    """Mock database query with result."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value.fetchone.return_value = {
        'person_id': 123,
        'name': 'Speaker Name',
        'party_id': 8,
    }
    
    with patch('sqlite3.connect', return_value=mock_connection):
        service = SpeakerService('db.db')
        speaker = service.get_speaker(123)
        assert speaker['name'] == 'Speaker Name'
```

### Mock Database Errors

```python
@pytest.mark.unit
def test_database_connection_error():
    """Mock database connection failure."""
    with patch('sqlite3.connect', side_effect=sqlite3.OperationalError('Connection failed')):
        with pytest.raises(sqlite3.OperationalError):
            PersonIndex('bad.db')

@pytest.mark.unit
def test_database_query_timeout():
    """Mock database timeout."""
    mock_db = MagicMock()
    mock_db.cursor.side_effect = sqlite3.ProgrammingError('Timeout')
    
    with patch('sqlite3.connect', return_value=mock_db):
        with pytest.raises(sqlite3.ProgrammingError):
            PersonIndex('slow.db')
```

---

## Configuration Mocking

### Mock ConfigValue Resolution

```python
from unittest.mock import patch, PropertyMock

@pytest.mark.unit
def test_load_corpus_path():
    """Mock configuration value resolution."""
    with patch('pyriksprot.configuration.ConfigValue.resolve') as mock_resolve:
        mock_resolve.return_value = '/path/to/corpus'
        
        path = ConfigValue('corpus.folder').resolve()
        assert path == '/path/to/corpus'
```

### Mock Environment Variables

```python
@pytest.mark.unit
def test_config_from_environment():
    """Mock environment variable configuration."""
    with patch.dict('os.environ', {'PYRIKSPROT_CORPUS_VERSION': 'v1.4.1'}):
        version = os.environ.get('PYRIKSPROT_CORPUS_VERSION')
        assert version == 'v1.4.1'

@pytest.mark.unit
def test_missing_required_env_var():
    """Mock missing required environment variable."""
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(KeyError):
            os.environ['REQUIRED_VAR']
```

### Mock YAML Config Parsing

```python
from unittest.mock import patch, mock_open

@pytest.mark.unit
def test_load_yaml_config():
    """Mock YAML file loading."""
    yaml_content = """
    corpus:
        version: v1.4.1
        folder: /path/to/corpus
    """
    
    with patch('builtins.open', mock_open(read_data=yaml_content)):
        config = load_config('config.yml')
        assert config['corpus']['version'] == 'v1.4.1'
```

---

## Service Mocking

### Mock Speaker Info Service

```python
from unittest.mock import MagicMock
from pyriksprot.metadata import SpeakerInfo

@pytest.mark.unit
def test_vrt_export_with_mock_speaker():
    """Mock SpeakerInfoService."""
    mock_service = MagicMock()
    mock_service.get_speaker_info.return_value = SpeakerInfo(
        speech_id='s-1',
        person_id='p-123',
        name='Speaker Name',
        gender_id=1,
        party_id=8,
        term_of_office=None,
    )
    
    export = VrtExportService(mock_service)
    vrt = export.to_vrt(protocol)
    
    assert 'Speaker Name' in vrt
    mock_service.get_speaker_info.assert_called()
```

### Mock Corpus Iterator Service

```python
@pytest.mark.unit
def test_extract_with_mock_corpus():
    """Mock corpus iteration."""
    mock_iterator = MagicMock()
    mock_iterator.__iter__.return_value = [
        MockProtocolSegment(name='prot-1', utterances=5),
        MockProtocolSegment(name='prot-2', utterances=7),
    ]
    
    with patch('pyriksprot.corpus.tagged.ProtocolIterator', return_value=mock_iterator):
        results = extract_corpus(source='dummy')
        assert len(results) == 2
```

---

## Time and Random Mocking

### Mock Current Date/Time

```python
from unittest.mock import patch
from datetime import datetime

@pytest.mark.unit
def test_with_fixed_time():
    """Mock time.time()."""
    fixed_time = datetime(2020, 1, 1, 12, 0, 0)
    
    with patch('datetime.datetime') as mock_datetime:
        mock_datetime.now.return_value = fixed_time
        result = get_timestamp()
        assert result.year == 2020
```

### Mock Random Values

```python
from unittest.mock import patch

@pytest.mark.unit
def test_with_fixed_random():
    """Mock random.random()."""
    with patch('random.random', return_value=0.5):
        value = get_random_number()
        assert value == 0.5
    
    with patch('random.choice', return_value='selected'):
        choice = select_random(['option1', 'option2', 'option3'])
        assert choice == 'selected'
```

---

## Multiprocessing Mocking

### Mock Dispatch Function

```python
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_parallel_processing():
    """Mock multiprocessing dispatch."""
    mock_results = [
        {'id': 1, 'result': 'data1'},
        {'id': 2, 'result': 'data2'},
    ]
    
    with patch('pyriksprot.dispatch.dispatch') as mock_dispatch:
        mock_dispatch.return_value = mock_results
        
        results = parallel_extract([1, 2, 3])
        assert len(results) == 2
        mock_dispatch.assert_called_once()
```

### Mock Pool.map()

```python
@pytest.mark.unit
def test_pool_processing():
    """Mock multiprocessing Pool."""
    mock_pool = MagicMock()
    mock_pool.map.return_value = ['result1', 'result2', 'result3']
    
    with patch('multiprocessing.Pool', return_value=mock_pool):
        results = process_in_parallel(data=[1, 2, 3])
        assert len(results) == 3
```

---

## Best Practices

### 1. Mock at the Right Level

```python
# ❌ WRONG - too high level
with patch('pyriksprot'):
    ...

# ✅ RIGHT - specific module/function
with patch('pyriksprot.corpus.tagged.load_protocol'):
    ...
```

### 2. Use Context Managers

```python
# ✅ GOOD - clear scope
with patch('module.function') as mock_func:
    my_code()
    mock_func.assert_called_once()

# ⚠️ OK but less clear - using decorator
@patch('module.function')
def test_function(mock_func):
    my_code()
    mock_func.assert_called_once()
```

### 3. Always Assert Mocks Were Used

```python
# ❌ WEAK - doesn't verify mock was used
with patch('os.makedirs'):
    create_output_dir()

# ✅ STRONG - verify mock was called correctly
with patch('os.makedirs') as mock_mkdir:
    create_output_dir()
    mock_mkdir.assert_called_once_with('/output', exist_ok=True)
```

### 4. Use Fixtures for Complex Mocks

```python
# ✅ GOOD - reusable fixture for complex setup
@pytest.fixture
def mock_speaker_service():
    service = MagicMock()
    service.get_speaker_info.return_value = SpeakerInfo(...)
    return service

@pytest.mark.unit
def test_with_fixture(mock_speaker_service):
    # Use the fixture
    export = VrtExportService(mock_speaker_service)
    ...
```

### 5. Document Why You're Mocking

```python
@pytest.mark.unit
def test_parse_handles_missing_file():
    """
    Mock file system to test error handling without needing
    actual files. Avoids tight coupling to file system state.
    """
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            parse_protocol('missing.xml')
```

---

## Common Patterns

### Pattern: Mock + Assert

```python
with patch('module.function') as mock_func:
    mock_func.return_value = 'expected'
    result = my_code()
    assert result == 'expected'
    mock_func.assert_called_with(expected_arg)
```

### Pattern: Mock + Side Effect

```python
with patch('module.function') as mock_func:
    mock_func.side_effect = [1, 2, 3]  # Returns different values
    assert my_code() == 1
    assert my_code() == 2
    assert my_code() == 3
```

### Pattern: Mock + Exception

```python
with patch('module.function') as mock_func:
    mock_func.side_effect = ValueError('Error')
    with pytest.raises(ValueError):
        my_code()
```

### Pattern: Mock + Verify Calls

```python
with patch('module.function') as mock_func:
    my_code(1, 2, 3)
    mock_func.assert_called_once_with(1, 2, 3)
    assert mock_func.call_count == 1
    assert mock_func.call_args == ((1, 2, 3), {})
```

---

## References

- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Mock Plugin](https://pytest-mock.readthedocs.io/)


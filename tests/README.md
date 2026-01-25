# Pyriksprot Test Suite

## Overview

The test suite is organized into two categories:

### Unit Tests (`tests/unit/`)
- **Fast**: Run in < 10 seconds total
- **Isolated**: No external dependencies (files, databases, network)
- **Repeatable**: Same results every time, independent of environment
- **Use**: Fast feedback during development, CI/CD validation

### Integration Tests (`tests/integration/`)
- **Comprehensive**: Test real-world scenarios with actual data
- **External Dependencies**: Require test corpora, metadata databases, etc.
- **Slower**: May take minutes to run
- **Use**: Validation before release, nightly CI/CD runs

## Running Tests

### Run all tests
```bash
pytest
```

### Run only unit tests (fast)
```bash
pytest -m unit
# Or with coverage:
pytest -m unit --cov
```

### Run only integration tests
```bash
pytest -m integration
```

### Run specific test file
```bash
pytest tests/unit/test_interface.py
pytest tests/integration/test_corpus_parsing.py
```

### Run specific test
```bash
pytest tests/unit/test_interface.py::test_utterance_checksum
```

### Run tests by module (respecting folder structure)
```bash
pytest tests/unit/configuration/
pytest tests/unit/corpus/
pytest tests/integration/metadata/
```

### Exclude slow tests
```bash
pytest -m "not slow"
```

### Run with different output
```bash
pytest -v                    # Verbose
pytest -x                    # Stop on first failure
pytest --lf                  # Run last failed
pytest -k "merge"            # Run tests matching name
```

## Test Structure

### Unit Tests (`tests/unit/`)

Located in `tests/unit/`, mirroring the source structure in `pyriksprot/`:

```
tests/unit/
├── conftest.py                    # Unit test fixtures
├── test_interface.py              # Core domain model tests
├── test_to_speech.py              # Speech generation logic
├── test_utility.py                # Utility function tests
├── test_preprocess.py             # Preprocessing logic
├── configuration/
│   ├── test_config.py             # Config loading/resolution
│   └── test_inject.py             # Dependency injection
├── corpus/
│   ├── test_corpus_index.py       # Corpus indexing
│   ├── test_iterate.py            # Segment iteration
│   ├── parlaclarin/
│   │   ├── test_parse.py          # XML parsing
│   │   ├── test_convert.py        # Format conversion
│   │   └── test_iterate.py        # Protocol iteration
│   └── tagged/
│       ├── test_persist.py        # Tagged data persistence
│       └── test_extract.py        # Tag extraction
├── metadata/
│   ├── test_person.py             # Person metadata
│   ├── test_database.py           # Database operations
│   ├── test_codecs.py             # Data codecs
│   └── test_schema.py             # Metadata schema
├── dispatch/
│   ├── test_dispatch.py           # Dispatch logic
│   └── test_merge.py              # Segment merging
├── workflows/
│   ├── test_extract_tags.py       # Tag extraction workflow
│   ├── test_extract_text.py       # Text extraction workflow
│   └── test_speech_index.py       # Speech index workflow
└── scripts/
    ├── test_riksprot2speech.py    # Speech generation CLI
    ├── test_riksprot2text.py      # Text extraction CLI
    └── test_metadata2db.py        # Metadata CLI
```

### Integration Tests (`tests/integration/`)

Located in `tests/integration/`, testing complete workflows with real data:

```
tests/integration/
├── conftest.py                    # Session-scoped fixtures
├── test_corpus_parsing.py         # Full parsing pipeline
├── test_metadata_generation.py    # Full metadata creation
├── test_speech_extraction.py      # Full speech extraction
├── configuration/
│   └── test_config_with_env.py    # Config with environment
├── corpus/
│   ├── parlaclarin/
│   │   └── test_parse_real_corpus.py
│   └── tagged/
│       └── test_extract_real_corpus.py
├── metadata/
│   ├── test_create_database.py    # Database creation
│   └── test_index_generation.py   # Index generation
├── workflows/
│   ├── test_end_to_end_workflows.py
│   └── test_cli_integration.py
└── infrastructure/
    └── test_data_setup.py         # Test data management
```

### Test Fixtures (`tests/fixtures/`)

Reusable test data and utilities:

```
tests/fixtures/
├── __init__.py
├── synthetic.py                   # In-memory fixtures for unit tests
├── sample_data.py                 # Small sample data for integration tests
└── builders.py                    # Test data builders
```

## Writing Tests

### Marking Tests

Always mark tests as either unit or integration:

```python
@pytest.mark.unit
def test_utterance_checksum():
    """Fast unit test with no external dependencies."""
    u = interface.Utterance(u_id="u1", who="speaker", paragraphs=["Text"])
    assert u.checksum() is not None


@pytest.mark.integration
def test_parse_protocol_from_disk(sample_corpus_dir):
    """Integration test requiring real files."""
    path = sample_corpus_dir / "prot-1995-test.xml"
    protocol = parse(str(path))
    assert len(protocol.utterances) > 0
```

### Unit Test Best Practices

1. **Use synthetic fixtures** from `tests/fixtures/synthetic.py`:
```python
from tests.fixtures.synthetic import minimal_protocol, minimal_utterance

@pytest.mark.unit
def test_protocol_merge(minimal_protocol):
    """Use in-memory fixtures for unit tests."""
    speeches = to_speeches(minimal_protocol, merge_strategy='chain')
    assert len(speeches) > 0
```

2. **Mock external operations**:
```python
from unittest.mock import patch, MagicMock, mock_open

@pytest.mark.unit
def test_load_protocol_handles_file_error():
    """Mock file I/O to test error handling."""
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            load_protocol('nonexistent.xml')

@pytest.mark.unit
def test_parse_with_mock_file():
    """Mock file content for testing."""
    fake_xml = '<?xml version="1.0"?><protocol></protocol>'
    with patch('builtins.open', mock_open(read_data=fake_xml)):
        result = parse_protocol('dummy.xml')
        assert result is not None
```

### Mocking Patterns

#### Mock File System Operations
```python
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_corpus_loader_creates_output_dir():
    """Mock os.makedirs to verify it was called."""
    with patch('os.makedirs') as mock_mkdir:
        load_corpus(output_dir='/tmp/out')
        mock_mkdir.assert_called_with('/tmp/out', exist_ok=True)

@pytest.mark.unit
def test_file_not_found_error():
    """Mock Path.exists to test missing file handling."""
    with patch('pathlib.Path.exists', return_value=False):
        with pytest.raises(FileNotFoundError):
            load_file('missing.txt')
```

#### Mock Database Connections
```python
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_person_index_queries_database():
    """Mock SQLite database connection."""
    mock_db = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = {
        'person_id': 1, 'name': 'John Doe', 'party': 'S'
    }
    
    with patch('pyriksprot.metadata.person.sqlite3.connect', return_value=mock_db):
        index = PersonIndex('dummy.db')
        result = index.get('john-doe')
        assert result['name'] == 'John Doe'

@pytest.mark.unit
def test_database_connection_error():
    """Mock database connection failure."""
    with patch('sqlite3.connect', side_effect=sqlite3.OperationalError('Connection failed')):
        with pytest.raises(sqlite3.OperationalError):
            PersonIndex('bad.db')
```

#### Mock External Services/APIs
```python
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_github_metadata_fetch():
    """Mock HTTP requests to GitHub API."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        'content': 'base64_encoded_csv_data'
    }
    
    with patch('requests.get', return_value=mock_response):
        data = fetch_github_metadata('repo/path')
        assert data is not None

@pytest.mark.unit
def test_network_timeout_handling():
    """Mock network timeout."""
    with patch('requests.get', side_effect=requests.Timeout('Connection timeout')):
        with pytest.raises(requests.Timeout):
            fetch_github_metadata('repo/path')
```

#### Mock Configuration Values
```python
from unittest.mock import patch, PropertyMock

@pytest.mark.unit
def test_config_value_resolution():
    """Mock ConfigValue to test config-dependent logic."""
    with patch('pyriksprot.configuration.ConfigValue.resolve') as mock_resolve:
        mock_resolve.return_value = '/path/to/corpus'
        
        path = ConfigValue('corpus.folder').resolve()
        assert path == '/path/to/corpus'

@pytest.mark.unit
def test_missing_config_value():
    """Mock missing configuration."""
    with patch.dict('os.environ', {}, clear=True):
        with pytest.raises(KeyError):
            ConfigValue('nonexistent.key').resolve()
```

#### Mock Multiprocessing
```python
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_dispatch_uses_correct_workers():
    """Mock multiprocessing dispatch."""
    with patch('pyriksprot.dispatch.dispatch') as mock_dispatch:
        mock_dispatch.return_value = [{'result': 'data'}]
        
        result = my_parallel_function(data=[1, 2, 3])
        
        # Verify dispatch was called with correct parameters
        mock_dispatch.assert_called()
        assert len(result) == 1
```

#### Mock Speaker Service
```python
from unittest.mock import MagicMock
from pyriksprot.metadata import SpeakerInfo

@pytest.mark.unit
def test_export_service_with_mock_speaker():
    """Mock SpeakerInfoService to test export logic."""
    mock_service = MagicMock()
    mock_service.get_speaker_info.return_value = SpeakerInfo(
        speech_id='s-1',
        person_id='p-123',
        name='Speaker Name',
        gender_id=1,
        party_id=8,
    )
    
    export_service = VrtExportService(mock_service)
    vrt = export_service.to_vrt(protocol)
    
    assert 'Speaker Name' in vrt
    mock_service.get_speaker_info.assert_called()
```

3. **Test pure logic**, not side effects:
```python
@pytest.mark.unit
def test_merge_strategy_chain():
    """Test merge logic without file I/O."""
    utterances = [
        build_utterance("u1", prev_id=None, next_id="u2"),
        build_utterance("u2", prev_id="u1", next_id=None),
    ]
    result = MergeByChain.group_utterances_by_chain(utterances)
    assert len(result) == 1  # All merged
```

### Integration Test Best Practices

1. **Use session-scoped fixtures** for shared setup:
```python
@pytest.mark.integration
def test_full_parsing_pipeline(source_index, person_index):
    """Use session-scoped fixtures that may be slow to create."""
    results = parse_corpus(source_index, person_index)
    assert len(results) > 0
```

2. **Test real workflows**:
```python
@pytest.mark.integration
def test_extract_tags_workflow(sample_tagged_corpus_dir, tmp_path):
    """Test complete extraction workflow."""
    result = extract_tags(
        source_folder=sample_tagged_corpus_dir,
        target_name=str(tmp_path / "output"),
    )
    assert (tmp_path / "output").exists()
```

3. **Skip gracefully if data missing**:
```python
@pytest.mark.integration
@pytest.mark.skipif(not sample_tagged_frames_exists(), reason="Test data not found")
def test_with_optional_data():
    """Safely skip if external data unavailable."""
    ...
```

## Fixtures

### Synthetic Fixtures (Unit Tests)

Located in `tests/fixtures/synthetic.py`, these are in-memory fixtures:

- `minimal_utterance` - Single utterance
- `utterance_sequence_same_speaker` - Multiple utterances, same speaker
- `utterance_sequence_different_speakers` - Multiple utterances, different speakers
- `minimal_protocol` - Basic protocol
- `protocol_single_speaker` - Protocol with one speaker
- `protocol_multiple_speakers` - Protocol with multiple speakers
- `minimal_tagged_frame` - Small POS-tagged text
- `tagged_frame_with_multiple_tokens` - Larger tagged frame

### Session Fixtures (Integration Tests)

Defined in `tests/conftest.py`:

- `source_index` - Corpus index for tagged frames
- `xml_source_index` - Corpus index for XML files
- `person_index` - Speaker metadata index
- `speaker_service` - Speaker info lookup service
- `tagged_speeches` - Pre-loaded tagged speeches

## Common Tasks

### Add a new unit test

1. Create test file in `tests/unit/` mirroring source structure
2. Mark as `@pytest.mark.unit`
3. Use synthetic fixtures from `tests/fixtures/synthetic.py`
4. Example:

```python
# tests/unit/test_my_feature.py
import pytest
from tests.fixtures.synthetic import minimal_protocol

@pytest.mark.unit
def test_my_feature(minimal_protocol):
    """Test my new feature."""
    result = my_feature(minimal_protocol)
    assert result is not None
```

### Add a new integration test

1. Create test file in `tests/integration/` mirroring source structure
2. Mark as `@pytest.mark.integration`
3. Use session-scoped fixtures from parent conftest
4. Example:

```python
# tests/integration/test_my_workflow.py
import pytest

@pytest.mark.integration
def test_my_workflow(source_index, tmp_path):
    """Test my workflow with real data."""
    result = my_workflow(source_index, output=str(tmp_path))
    assert result.success
```

### Create custom test data

Use builders from `tests/fixtures/synthetic.py`:

```python
from tests.fixtures.synthetic import build_protocol, build_utterance

@pytest.mark.unit
def test_custom_scenario():
    """Test a specific scenario."""
    utterances = [
        build_utterance("u1", who="speaker_a"),
        build_utterance("u2", who="speaker_b"),
    ]
    protocol = build_protocol(utterances=utterances)
    assert len(protocol.utterances) == 2
```

## Mocking Best Practices

### When to Mock

✅ **DO Mock**:
- File system operations (open, read, write)
- Database connections
- Network requests (HTTP, APIs)
- External services
- Configuration loading
- Time/datetime operations
- Random number generation
- Multiprocessing dispatch

❌ **DON'T Mock**:
- Internal logic (merge strategies, parsing)
- Pure functions without side effects
- Domain models (Utterance, Protocol, Speech)
- Fixtures - use synthetic fixtures instead

### Mock Patterns

**Pattern 1: Context Manager (Recommended)**
```python
@pytest.mark.unit
def test_with_mock_context():
    """Use 'with' for clear mock scope."""
    with patch('module.function') as mock_func:
        mock_func.return_value = 'expected'
        result = my_code()
        assert result == 'expected'
        mock_func.assert_called_once()
```

**Pattern 2: Decorator (For Multiple Tests)**
```python
@pytest.mark.unit
@patch('module.slow_function', return_value='fast')
def test_with_mock_decorator(mock_func):
    """Use decorator to mock for entire test."""
    result = my_code()
    assert result == 'fast'
```

**Pattern 3: Fixture (For Shared Mocks)**
```python
@pytest.fixture
def mock_database():
    """Reusable mock database."""
    with patch('sqlite3.connect') as mock_db:
        yield mock_db

@pytest.mark.unit
def test_with_mock_fixture(mock_database):
    """Use fixture for complex or shared mocks."""
    mock_database.execute.return_value = []
    result = query_database()
    assert result == []
```

### Mock Return Values

```python
from unittest.mock import MagicMock, PropertyMock

# Simple return value
mock_func.return_value = 'result'

# Return different values on successive calls
mock_func.side_effect = ['first', 'second', 'third']

# Raise exception
mock_func.side_effect = ValueError('Error message')

# Mock property
type(mock_obj).property_name = PropertyMock(return_value='value')

# Call with arguments
mock_func('arg1', 'arg2')
mock_func.assert_called_with('arg1', 'arg2')

# Called at least once
mock_func.assert_called()

# Called exactly once
mock_func.assert_called_once()

# Not called
mock_func.assert_not_called()

# Call count
assert mock_func.call_count == 5
```

### Common Gotchas

**Gotcha 1: Patching the wrong location**
```python
# ❌ WRONG - patch where it's defined
with patch('pyriksprot.interface.Utterance'):
    ...

# ✅ RIGHT - patch where it's used
with patch('pyriksprot.to_speech.Utterance'):
    ...
```

**Gotcha 2: Not mocking dependencies**
```python
# ❌ WRONG - FileNotFoundError still raised by open()
with patch('load_protocol'):
    result = load_protocol('file.xml')

# ✅ RIGHT - Mock the actual operation
with patch('builtins.open', side_effect=FileNotFoundError):
    with pytest.raises(FileNotFoundError):
        load_protocol('file.xml')
```

**Gotcha 3: Forgetting to assert mocks were called**
```python
# ❌ WEAK - doesn't verify mock was used
with patch('os.makedirs') as mock_mkdir:
    my_code()

# ✅ STRONG - verify mock was called as expected
with patch('os.makedirs') as mock_mkdir:
    my_code()
    mock_mkdir.assert_called_once_with('/path', exist_ok=True)
```

## Troubleshooting

### Unit tests fail with "ConfigStore not configured"

**Problem**: You need to initialize configuration before using ConfigValue

**Solution**: Either:
1. Add test marker and move to integration tests
2. Mock the configuration in unit test:
```python
@pytest.mark.unit
def test_with_mocked_config():
    with patch('pyriksprot.configuration.ConfigValue.resolve') as mock_resolve:
        mock_resolve.return_value = '/path/to/corpus'
        # Now ConfigValue().resolve() works
        result = my_function()
```
3. Use synthetic fixtures that don't require configuration

### Mock is not being called

**Problem**: Mock isn't being used, likely wrong patch path

**Solution**: 
1. Verify you're patching where the function is used, not defined
2. Use `assert_called()` or `call_count` to check:
```python
with patch('module.function') as mock_f:
    my_code()
    print(f"Called {mock_f.call_count} times")
    if not mock_f.called:
        print("Mock was never called!")
```
3. Check that your code actually calls the function

### "Module has no attribute" error when patching

**Problem**: Path to patch is incorrect

**Solution**:
1. Use full module path: `patch('pyriksprot.module.function')`
2. Import and check the function exists:
```python
from pyriksprot.module import function
# Then patch it where it's used:
with patch('pyriksprot.other_module.function'):
```

**Problem**: Integration tests skip because external data missing

**Solution**: 
1. Run `make setup-test-data` to download test corpora
2. Set environment variables: `CORPUS_VERSION`, `METADATA_VERSION`
3. Check `tests/test_data/` exists and is populated

### Tests run slowly

**Problem**: You're running integration tests

**Solution**:
1. Use `pytest -m unit` for fast feedback during development
2. Run integration tests separately: `pytest -m integration`
3. Add `@pytest.mark.slow` to known slow tests

## CI/CD Integration

### Fast validation (unit tests only)
```bash
pytest -m unit --cov=pyriksprot --cov-report=term-missing
```

### Nightly comprehensive test
```bash
pytest --cov=pyriksprot --cov-report=html
```

### Pre-commit hook
```bash
pytest -m unit -x  # Stop on first failure
```

## References

- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest Markers](https://docs.pytest.org/en/stable/how-to-use-markers.html)
- [Testing Best Practices](https://docs.pytest.org/en/stable/goodpractices.html)

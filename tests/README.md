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
from unittest.mock import patch, MagicMock

@pytest.mark.unit
def test_load_protocol_handles_file_error():
    """Mock file I/O to test error handling."""
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            load_protocol('nonexistent.xml')
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

## Troubleshooting

### Unit tests fail with "ConfigStore not configured"

**Problem**: You need to initialize configuration before using ConfigValue

**Solution**: Either:
1. Add test marker and move to integration tests
2. Mock the configuration in unit test
3. Use synthetic fixtures that don't require configuration

### Test data not found

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

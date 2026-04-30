# Unit Test Quality Review - Pyriksprot

**Review Date:** January 24, 2026  
**Reviewer:** AI Coding Agent  
**Project:** Pyriksprot v2.0.0  
**Test Framework:** pytest

## Executive Summary

The project has **23 test files** covering **82 source files** (~28% file coverage), with an overall line coverage of **68.8%**. While the core domain logic (parsing, speech extraction, interfaces) has good coverage, the test suite exhibits significant structural issues:

- ❌ **No separation** between unit tests and integration tests
- ❌ **Poor isolation**: Heavy reliance on external test data and fixtures
- ⚠️ **Inconsistent structure**: Test folder doesn't fully mirror source structure
- ⚠️ **Missing coverage**: Workflows (11 files), scripts (20 files), and configuration modules undertested
- ✅ **Good coverage**: Core domain models (`interface.py` 90.7%, `to_speech.py` 91.2%)
- ✅ **Strong fixtures**: Well-designed session-scoped fixtures in `conftest.py`

**Overall Grade: C+**  
The tests work but require significant refactoring to meet modern standards for maintainability, isolation, and clarity.

---

## Detailed Analysis

### 1. Test Organization & Structure

#### 1.1 Current Structure
```
tests/
├── conftest.py                    # Global fixtures
├── *_test.py                      # 11 root-level test files
├── fakes.py                       # Test data utilities
├── utility.py                     # Test infrastructure helpers
├── metadata/                      # 3 test files
├── parlaclarin/                   # 5 test files + utilities
├── tagged_corpus/                 # 2 test files
├── scripts/                       # 1 helper, NO actual tests
└── (no workflows/ or configuration/ test subdirs)
```

#### 1.2 Source Structure (Incompletely Mirrored)
```
pyriksprot/
├── configuration/                 # 2 files → 1 test file (config_test.py)
├── corpus/
│   ├── parlaclarin/              # 5 files → 5 tests ✓
│   └── tagged/                   # 4 files → 2 tests ⚠️
├── metadata/                      # 11 files → 3 tests ⚠️
├── workflows/                     # 11 files → 0 tests ❌
├── scripts/                       # 20 files → 0 tests ❌
├── dispatch/                      # ~4 files → 1 test file ⚠️
└── dehyphenation/                # ? files → 1 test file ⚠️
```

**Issues:**
- Root-level test files (`interface_test.py`, `merge_test.py`, etc.) don't follow folder hierarchy
- **Critical gap**: No `tests/workflows/` directory despite 11 workflow files
- **Critical gap**: No actual script tests despite 20 CLI scripts
- Test structure doesn't clearly communicate what's being tested

**Recommendation:** Reorganize to mirror source structure exactly.

---

### 2. Unit Test vs Integration Test Separation

#### 2.1 Current State: **NO SEPARATION**

**Observations:**
- No `@pytest.mark.unit` or `@pytest.mark.integration` markers found
- Tests mix isolated logic testing with:
  - File I/O operations
  - External data dependencies (riksdagen-records GitHub repo)
  - Database creation/queries
  - Multi-file corpus processing
  
**Examples of Mixed Concerns:**

| Test File | Mixed Behaviors |
|-----------|----------------|
| `interface_test.py` | Unit tests for Utterance + corpus file loading |
| `parse_test.py` | XML parsing logic + file system dependencies |
| `dispatch_test.py` | Dispatcher logic + actual file/zip creation |
| `metadata/generate_test.py` | Database creation + GitHub API calls |
| `infrastructure_test.py` | Explicitly tests infrastructure setup (should be isolated) |

#### 2.2 Dependency on External Test Data

**Heavy reliance on external corpora:**
```python
# tests/conftest.py bootstrap
ensure_test_corpora_exist(only_check=True)  # Requires external data
```

**Test data sources:**
- `humlab-swedeb/sample-data` repo (external)
- `tests/test_data/` (local fixtures)
- ParlaCLARIN XML files from riksdagen-records
- Tagged frames corpus (requires pre-tagging)
- Metadata database (requires download + processing)

**Impact:**
- Many tests skip if data not available (`@pytest.mark.skipif`)
- Bootstrap failure prevents test runs
- Difficult to run tests in CI/CD without elaborate setup
- Slow test execution (I/O bound)

**Recommendation:** 
- Create minimal synthetic test data for unit tests
- Separate integration tests that require real corpora
- Use mocking/fixtures for file operations in unit tests

---

### 3. Test Isolation & Independence

#### 3.1 Session-Scoped Fixtures (Good Practice)
```python
@pytest.fixture(scope='session')
def source_index() -> CorpusSourceIndex:
    # Shared across all tests
    
@pytest.fixture(scope='session')
def person_index() -> PersonIndex:
    # Loaded once from database
```

**Pros:** Efficient for expensive setup  
**Cons:** Tests share state, can't run in isolation

#### 3.2 Shared Test Data Files
- `tests/test_data/test_documents.txt` - list of protocols
- `tests/test_data/fake-*.xml` - sample XML files
- `tests/output/` - shared output directory (potential conflicts)

**Issues:**
- Tests writing to same output directory can interfere
- `uuid.uuid1()[:8]` used for unique filenames (potential collisions)
- No cleanup between test runs visible in many tests
- `shutil.rmtree(source_folder, ignore_errors=True)` - silent failures

#### 3.3 Mock/Patch Usage

**Finding:** Almost NO use of mocking
```bash
$ grep -r "@mock\|@patch\|MagicMock" tests/ | wc -l
1  # Only 1 occurrence in generate_test.py
```

**Impact:**
- Tests hit real file system, databases, network
- Slower execution
- Harder to test error paths
- Brittle tests (dependent on external state)

**Recommendation:** Introduce mocking for:
- File I/O operations
- GitHub API calls
- Database connections
- External process execution

---

### 4. Test Coverage Analysis

#### 4.1 Coverage by Module (from coverage.xml)

| Module | Line Coverage | Branch Coverage | Status |
|--------|---------------|-----------------|--------|
| **Core Domain** | | | |
| `interface.py` | 90.7% | 57.1% | ✅ Excellent |
| `to_speech.py` | 91.2% | 77.3% | ✅ Excellent |
| `preprocess.py` | 75.6% | 33.3% | ⚠️ Good, needs branch tests |
| **Corpus** | | | |
| `corpus/iterate.py` | 92.5% | 73.1% | ✅ Excellent |
| `corpus/corpus_index.py` | 87.4% | 80.0% | ✅ Excellent |
| `corpus/parlaclarin/parse.py` | 90.3% | 82.5% | ✅ Excellent |
| `corpus/parlaclarin/convert.py` | 47.2% | 10.0% | ❌ Poor |
| **Configuration** | | | |
| `configuration/config.py` | 74.2% | 40.0% | ⚠️ Fair |
| `configuration/inject.py` | 78.8% | 40.0% | ⚠️ Fair |
| **Metadata** | | | |
| (multiple files) | Varies | Varies | ⚠️ Inconsistent |
| **Other** | | | |
| `gitchen.py` | 38.5% | 17.7% | ❌ Poor |
| `utility.py` | 71.1% | 59.7% | ⚠️ Fair |

**Overall:** 68.8% line coverage, 54.4% branch coverage

#### 4.2 Critical Gaps

**1. Workflows (0% explicit coverage)**
```
pyriksprot/workflows/
├── extract_tags.py          ❌ No tests
├── extract_text.py          ❌ No tests
├── create_metadata.py       ⚠️ Called in metadata tests (indirect)
├── speech_index.py          ❌ No tests
├── export_vrt.py            ❌ No tests
├── subset_corpus.py         ⚠️ Used in test utilities
└── (7 other workflow files) ❌ No dedicated tests
```

**2. Scripts (0% explicit coverage)**
```
pyriksprot/scripts/
├── riksprot2speech.py       ❌ No tests
├── riksprot2text.py         ❌ No tests
├── riksprot2vrt.py          ❌ No tests
├── metadata2db.py           ❌ No tests
└── (16 other scripts)       ❌ No tests
```

**3. Configuration System**
- Only `config_test.py` tests basic YAML loading
- No tests for `ConfigValue.resolve()` edge cases
- No tests for environment variable overrides
- No tests for custom YAML constructors (`!jj`, `!join`, `!path_join`)

**4. Dispatch Module**
- `dispatch_test.py` tests dispatchers
- No tests for dispatch strategies, item creation, or error handling

---

### 5. Test Quality & Best Practices

#### 5.1 Strengths ✅

1. **Parametrized tests** used extensively:
```python
@pytest.mark.parametrize(
    'filename, u_count, intro_count',
    [("prot-1933--fk--005.xml", 0, 1), ...]
)
def test_parlaclarin_n_speaker_notes(filename, u_count, intro_count):
```

2. **Fixtures for common data** (`utterances`, `source_index`, `person_index`)

3. **Helper utilities** (`fakes.py`, `tests/utility.py`)

4. **Descriptive test names** (mostly clear intent)

5. **Test data versioning** via `test_documents.txt`

#### 5.2 Weaknesses ❌

1. **No docstrings** in test functions (unclear intent for complex tests)

2. **Commented-out tests** (e.g., `bug_test.py` - entire file commented)

3. **Inconsistent assertion patterns**:
```python
# Sometimes detailed:
assert [s.who for s in speeches] == [s.who for s in expected]
assert [s.text for s in speeches] == [s.text for s in expected]

# Sometimes vague:
assert True  # In test_gh_fetch_metadata_folder
```

4. **Magic values** without explanation:
```python
def test_parlaclarin_n_speaker_notes(..., u_count: int, intro_count: int):
    # No comment on why these specific counts
```

5. **Test infrastructure mixed with tests**:
```python
@pytest.mark.skipif(not FORCE_RUN_SKIPS, reason="Test infrastructure test")
def test_setup_sample_tagged_frames_corpus(...):
    # This is setup, not a test!
```

6. **Brittle skip conditions**:
```python
@pytest.mark.skipif(condition=sample_parlaclarin_corpus_exists(), reason="Test data found")
def test_setup_sample_xml_corpus(...):
    # Inverted logic - skips when data EXISTS
```

---

### 6. Consistency Issues

#### 6.1 Naming Conventions

**Inconsistent test file naming:**
- `*_test.py` ✅ (20 files)
- `bug_test.py` ⚠️ (not descriptive, entire file commented)
- `fakes.py` ⚠️ (not a test, should be `fixtures.py`)

#### 6.2 Import Patterns

**Inconsistent aliasing:**
```python
# Some files:
from os.path import join as jj
jj = os.path.join  # Redundant

# Others:
import os
os.path.join(...)

# Better pattern:
from pathlib import Path
```

#### 6.3 Fixture Scopes

**Inconsistent scoping:**
- Most conftest fixtures: `scope='session'`
- Test-specific fixtures: mix of `scope='module'` and no scope
- No clear guidelines on when to use each

---

## Improvement Plan (Ordered by Impact)

### **Priority 1: HIGH IMPACT - Immediate Action Required**

#### Task 1.1: Separate Unit and Integration Tests (Impact: ⭐⭐⭐⭐⭐)
**Effort:** 2-3 days  
**Rationale:** Enables fast CI/CD, parallel test execution, better debugging

**Actions:**
1. Create test markers:
```python
# pyproject.toml
[tool.pytest.ini_options]
markers = [
    "unit: Fast, isolated unit tests (no I/O)",
    "integration: Tests requiring external data/services",
    "slow: Slow-running tests",
]
```

2. Reorganize test files:
```
tests/
├── unit/                          # NEW: Pure unit tests
│   ├── test_interface.py          # Pure logic tests
│   ├── test_to_speech.py          # Merge strategies
│   ├── test_utility.py            # Helper functions
│   └── configuration/
│       └── test_config.py         # YAML parsing, resolution
├── integration/                   # NEW: Integration tests
│   ├── test_parlaclarin_parsing.py
│   ├── test_corpus_extraction.py
│   ├── test_metadata_generation.py
│   └── test_workflows.py
└── conftest.py                    # Shared fixtures
```

3. Add markers to existing tests:
```python
# Unit test example
@pytest.mark.unit
def test_utterance_checksum():
    u = interface.Utterance(u_id="u", who="x", paragraphs=["X"])
    assert u.checksum() == 'expected_hash'

# Integration test example
@pytest.mark.integration
def test_parse_protocol_from_file(tmp_path):
    # Uses real XML parsing, file I/O
```

4. Update CI/CD:
```yaml
# .github/workflows/test.yml
- name: Unit tests
  run: pytest -m unit --cov

- name: Integration tests  
  run: pytest -m integration
```

**Success Metrics:**
- Unit tests run in < 10 seconds
- Can run `pytest -m unit` without external dependencies
- Integration tests clearly marked and skippable

---

#### Task 1.2: Create Minimal Synthetic Test Data (Impact: ⭐⭐⭐⭐⭐)
**Effort:** 1-2 days  
**Rationale:** Eliminates external dependencies, speeds up tests, enables true isolation

**Actions:**
1. Create synthetic test fixtures:
```python
# tests/unit/fixtures/synthetic.py
@pytest.fixture
def minimal_protocol() -> interface.Protocol:
    """Minimal valid protocol for unit testing."""
    return interface.Protocol(
        date="2000",
        name="prot-2000-test",
        utterances=[
            interface.Utterance(
                u_id="u1", who="speaker_a", 
                speaker_note_id="n1", paragraphs=["Text A"]
            ),
            interface.Utterance(
                u_id="u2", who="speaker_a",
                speaker_note_id="n1", paragraphs=["Text B"]
            ),
        ],
        speaker_notes={"n1": "Speaker A intro"},
        page_references=[],
    )

@pytest.fixture
def minimal_tagged_frame() -> pd.DataFrame:
    """Minimal tagged frame for testing."""
    return pd.DataFrame({
        'token': ['Word1', 'word2', '.'],
        'pos': ['NN', 'VB', 'MAD'],
        'lemma': ['word1', 'word2', '.'],
    })
```

2. Replace file-based tests with in-memory tests:
```python
# Before (integration):
def test_parse_protocol():
    protocol = parse('tests/data/prot-1958.xml')
    assert len(protocol.utterances) > 0

# After (unit):
@pytest.mark.unit
def test_parse_protocol(minimal_xml_string):
    protocol = parse_from_string(minimal_xml_string)
    assert len(protocol.utterances) == 2
```

3. Mock file I/O in unit tests:
```python
from unittest.mock import mock_open, patch

@pytest.mark.unit
def test_load_protocol_handles_missing_file():
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            load_protocol('nonexistent.xml')
```

**Success Metrics:**
- Unit tests require no external files
- Test suite runs successfully on fresh clone
- Reduced test data size from GB to KB

---

#### Task 1.3: Add Workflow Tests (Impact: ⭐⭐⭐⭐)
**Effort:** 3-4 days  
**Rationale:** Workflows are critical business logic, currently untested

**Actions:**
1. Create test structure:
```
tests/
├── unit/
│   └── workflows/
│       ├── test_extract_tags.py
│       ├── test_extract_text.py
│       ├── test_create_metadata.py
│       └── test_speech_index.py
└── integration/
    └── workflows/
        └── test_end_to_end_workflows.py
```

2. **Unit tests** for workflow components:
```python
# tests/unit/workflows/test_extract_tags.py
@pytest.mark.unit
def test_extract_corpus_tags_validates_input():
    with pytest.raises(ValueError, match="source_folder required"):
        extract_tags.extract_corpus_tags(
            source_folder=None,  # Invalid
            ...
        )

@pytest.mark.unit  
def test_build_segment_iterator_with_invalid_merge_strategy():
    with pytest.raises(ValueError):
        extract_tags._build_segment_iterator(
            merge_strategy="invalid_strategy"
        )
```

3. **Integration tests** for full workflows:
```python
# tests/integration/workflows/test_extract_tags.py
@pytest.mark.integration
def test_extract_tags_workflow_end_to_end(tmp_path):
    """Test full extraction pipeline with sample data."""
    result = extract_tags.extract_corpus_tags(
        source_folder=sample_tagged_frames_dir,
        target_name=str(tmp_path / "output"),
        merge_strategy='chain',
        ...
    )
    
    assert result.success
    assert (tmp_path / "output").exists()
    # Validate output structure
```

**Success Metrics:**
- All workflow files have dedicated tests
- Coverage for workflows > 70%
- Edge cases (missing files, invalid params) covered

---

### **Priority 2: MEDIUM IMPACT - Important Improvements**

#### Task 2.1: Add Script Tests (Impact: ⭐⭐⭐⭐)
**Effort:** 4-5 days  
**Rationale:** Scripts are user-facing entry points, need validation

**Actions:**
1. Create script test structure:
```
tests/
├── unit/
│   └── scripts/
│       ├── test_riksprot2speech.py
│       ├── test_riksprot2text.py
│       ├── test_metadata2db.py
│       └── ...
└── integration/
    └── scripts/
        └── test_cli_integration.py
```

2. **Unit tests** for argument parsing and validation:
```python
# tests/unit/scripts/test_riksprot2text.py
from click.testing import CliRunner
from pyriksprot.scripts.riksprot2text import main

@pytest.mark.unit
def test_riksprot2text_validates_required_args():
    runner = CliRunner()
    result = runner.invoke(main, [])  # No args
    assert result.exit_code != 0
    assert "Missing argument" in result.output

@pytest.mark.unit
def test_riksprot2text_options_file_overrides():
    runner = CliRunner()
    with runner.isolated_filesystem():
        # Test --options-filename behavior
        ...
```

3. **Integration tests** for CLI workflows:
```python
@pytest.mark.integration
def test_riksprot2speech_full_workflow(sample_corpus_dir, tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, [
        '--source-folder', sample_corpus_dir,
        '--output-folder', str(tmp_path),
        '--merge-strategy', 'chain',
    ])
    assert result.exit_code == 0
    assert (tmp_path / 'speeches.feather').exists()
```

**Success Metrics:**
- All CLI scripts have basic argument validation tests
- Error messages tested
- Major scripts have end-to-end integration tests

---

#### Task 2.2: Improve Configuration Testing (Impact: ⭐⭐⭐)
**Effort:** 1-2 days  
**Rationale:** Configuration is foundational, currently under-tested

**Actions:**
1. Expand `config_test.py`:
```python
@pytest.mark.unit
def test_config_value_resolve_nested():
    config = Config.load(source="""
        section:
            subsection:
                value: "nested"
    """)
    ConfigStore.configure_context(source=config)
    assert ConfigValue("section.subsection.value").resolve() == "nested"

@pytest.mark.unit
def test_config_value_env_override():
    with mock.patch.dict(os.environ, {"PYRIKSPROT_TEST_VAR": "from_env"}):
        config = Config.load(source="test_var: from_yaml")
        ConfigStore.configure_context(source=config, env_prefix="PYRIKSPROT")
        assert ConfigValue("test_var").resolve() == "from_env"

@pytest.mark.unit
def test_config_custom_constructor_jj():
    config = Config.load(source="""
        path: !jj ["/root", "subdir", "file.txt"]
    """)
    assert config.get("path") == "/root/subdir/file.txt"
```

2. Test edge cases:
```python
@pytest.mark.unit
def test_config_value_missing_key():
    ConfigStore.configure_context(source="key: value")
    with pytest.raises(KeyError):
        ConfigValue("nonexistent").resolve()

@pytest.mark.unit
def test_config_circular_reference():
    # Test handling of circular refs in YAML
```

**Success Metrics:**
- Configuration coverage > 85%
- All custom constructors tested
- Environment override behavior validated

---

#### Task 2.3: Restructure Test Folder to Mirror Source (Impact: ⭐⭐⭐)
**Effort:** 1 day  
**Rationale:** Improves discoverability, maintainability, navigation

**Actions:**
1. Reorganize test folder:
```
tests/
├── conftest.py
├── fixtures/                      # NEW: Centralized test data
│   ├── synthetic.py
│   ├── sample_protocols.py
│   └── sample_metadata.py
├── unit/
│   ├── test_interface.py          # Mirrors pyriksprot/interface.py
│   ├── test_to_speech.py          # Mirrors pyriksprot/to_speech.py
│   ├── test_utility.py            # Mirrors pyriksprot/utility.py
│   ├── configuration/
│   │   ├── test_config.py
│   │   └── test_inject.py
│   ├── corpus/
│   │   ├── test_corpus_index.py
│   │   ├── test_iterate.py
│   │   ├── parlaclarin/
│   │   │   ├── test_parse.py
│   │   │   ├── test_convert.py
│   │   │   └── test_iterate.py
│   │   └── tagged/
│   │       ├── test_persist.py
│   │       └── test_extract.py
│   ├── metadata/
│   │   ├── test_person.py
│   │   ├── test_database.py
│   │   └── test_codecs.py
│   ├── dispatch/
│   │   ├── test_dispatch.py
│   │   └── test_merge.py
│   ├── workflows/
│   │   └── ... (as in Task 1.3)
│   └── scripts/
│       └── ... (as in Task 2.1)
└── integration/
    └── ...
```

2. Update imports in tests to reflect new structure

3. Document structure in `tests/README.md`

**Success Metrics:**
- 1:1 mapping between `pyriksprot/` and `tests/unit/`
- Easy to find tests for any source file
- Consistent organization

---

### **Priority 3: POLISH & MAINTENANCE**

#### Task 3.1: Introduce Mocking for External Dependencies (Impact: ⭐⭐⭐)
**Effort:** Ongoing (2-3 days initial)

**Actions:**
1. Mock GitHub API calls:
```python
@pytest.mark.unit
@patch('pyriksprot.metadata.download.requests.get')
def test_gh_download_files_handles_404(mock_get):
    mock_get.return_value.status_code = 404
    with pytest.raises(FileNotFoundError):
        gh_download_files(...)
```

2. Mock file system operations:
```python
from unittest.mock import MagicMock, patch

@pytest.mark.unit
@patch('pyriksprot.corpus.corpus_index.glob.glob')
def test_corpus_source_index_load_handles_empty_dir(mock_glob):
    mock_glob.return_value = []
    index = CorpusSourceIndex.load(...)
    assert len(index.paths) == 0
```

3. Mock database connections:
```python
@pytest.mark.unit
@patch('pyriksprot.metadata.database.sqlite3.connect')
def test_database_interface_connection_error(mock_connect):
    mock_connect.side_effect = sqlite3.Error("Connection failed")
    with pytest.raises(sqlite3.Error):
        DatabaseInterface(...)
```

---

#### Task 3.2: Add Test Documentation (Impact: ⭐⭐)
**Effort:** 1 day

**Actions:**
1. Create `tests/README.md`:
```markdown
# Pyriksprot Test Suite

## Running Tests

### All tests
```bash
pytest
```

### Unit tests only (fast, no external dependencies)
```bash
pytest -m unit
```

### Integration tests only
```bash
pytest -m integration
```

### Specific module
```bash
pytest tests/unit/corpus/
```

## Test Structure
- `unit/`: Fast, isolated tests (no I/O)
- `integration/`: Tests with external dependencies
- `fixtures/`: Shared test data and fixtures

## Adding Tests
1. Mirror source structure in `tests/unit/`
2. Mark tests: `@pytest.mark.unit` or `@pytest.mark.integration`
3. Use synthetic fixtures for unit tests
4. Document complex test scenarios

## Test Data
- Unit tests use synthetic fixtures from `tests/fixtures/`
- Integration tests use `tests/test_data/` (minimal sample corpus)
```

2. Add docstrings to complex tests:
```python
def test_merge_by_chain_with_consecutive_unknowns():
    """
    Test that MergeByChain correctly groups utterances when:
    1. Multiple 'unknown' speakers appear consecutively
    2. They share the same speaker_note_id
    3. merge_consecutive_unknowns=True
    
    Expected: All consecutive unknowns merged into single speech.
    """
    ...
```

---

#### Task 3.3: Clean Up Test Anti-Patterns (Impact: ⭐⭐)
**Effort:** 2 days

**Actions:**
1. Remove/fix commented-out tests:
```python
# bug_test.py - entire file commented
# Either delete or uncomment + fix
```

2. Replace vague assertions:
```python
# Before:
assert True

# After:
assert metadata_files_exist(target_folder)
assert_files_match_schema(target_folder, schema)
```

3. Add cleanup for test artifacts:
```python
@pytest.fixture
def temp_output_dir(tmp_path):
    """Isolated temp directory, auto-cleaned."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    yield output_dir
    # Automatic cleanup by pytest tmp_path
```

4. Fix inverted skip logic:
```python
# Before (confusing):
@pytest.mark.skipif(condition=corpus_exists(), reason="Test data found")

# After:
@pytest.mark.skipif(not corpus_exists(), reason="Test data not found")
```

---

#### Task 3.4: Improve Coverage Reporting (Impact: ⭐⭐)
**Effort:** 0.5 days

**Actions:**
1. Add coverage thresholds:
```toml
# pyproject.toml
[tool.coverage.run]
source = ["pyriksprot"]
omit = ["*/tests/*", "*/foss/*"]

[tool.coverage.report]
fail_under = 70
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise NotImplementedError",
    "if TYPE_CHECKING:",
]
```

2. Generate HTML coverage reports:
```bash
pytest --cov --cov-report=html
```

3. Add coverage badge to README

---

### **Priority 4: ADVANCED IMPROVEMENTS**

#### Task 4.1: Property-Based Testing for Core Logic (Impact: ⭐⭐)
**Effort:** 2-3 days

Use Hypothesis for property-based testing:
```python
from hypothesis import given, strategies as st

@given(
    utterances=st.lists(
        st.builds(
            interface.Utterance,
            u_id=st.text(min_size=1),
            who=st.text(min_size=1),
            speaker_note_id=st.text(min_size=1),
        ),
        min_size=1,
    )
)
@pytest.mark.unit
def test_merge_preserves_utterance_count(utterances):
    """Property: Merging should not lose utterances."""
    speeches = to_speech.to_speeches(utterances, merge_strategy='chain')
    total_utterances = sum(len(s.utterances) for s in speeches)
    assert total_utterances == len(utterances)
```

---

#### Task 4.2: Mutation Testing (Impact: ⭐)
**Effort:** 1 day setup + ongoing

Use `mutmut` to test test quality:
```bash
pip install mutmut
mutmut run
mutmut results
```

Identifies weak tests that don't catch bugs.

---

## Summary of Improvement Tasks

| Priority | Task | Impact | Effort | Coverage Gain |
|----------|------|--------|--------|---------------|
| **P1** | **1.1 Separate unit/integration** | ⭐⭐⭐⭐⭐ | 2-3 days | - |
| **P1** | **1.2 Synthetic test data** | ⭐⭐⭐⭐⭐ | 1-2 days | - |
| **P1** | **1.3 Workflow tests** | ⭐⭐⭐⭐ | 3-4 days | +15% |
| **P2** | **2.1 Script tests** | ⭐⭐⭐⭐ | 4-5 days | +10% |
| **P2** | **2.2 Configuration tests** | ⭐⭐⭐ | 1-2 days | +5% |
| **P2** | **2.3 Restructure folders** | ⭐⭐⭐ | 1 day | - |
| **P3** | **3.1 Introduce mocking** | ⭐⭐⭐ | 2-3 days | +5% |
| **P3** | **3.2 Test documentation** | ⭐⭐ | 1 day | - |
| **P3** | **3.3 Clean up anti-patterns** | ⭐⭐ | 2 days | - |
| **P3** | **3.4 Coverage reporting** | ⭐⭐ | 0.5 days | - |
| **P4** | **4.1 Property-based testing** | ⭐⭐ | 2-3 days | +3% |
| **P4** | **4.2 Mutation testing** | ⭐ | 1 day | - |

**Total Estimated Effort:** 22-30 days (1-1.5 months with dedicated focus)  
**Expected Coverage Improvement:** 68.8% → 85%+  
**Expected Test Speed Improvement:** 10x faster unit tests (< 10s vs current ~2-3min)

---

## Recommended Execution Sequence

### Phase 1: Foundation (Week 1-2)
1. Task 1.1: Separate unit/integration tests
2. Task 1.2: Create synthetic test data
3. Task 2.3: Restructure test folders

**Outcome:** Clean test architecture, fast unit tests runnable

### Phase 2: Coverage (Week 3-4)
4. Task 1.3: Add workflow tests
5. Task 2.1: Add script tests
6. Task 2.2: Improve configuration tests

**Outcome:** Major coverage gaps filled (85%+ coverage)

### Phase 3: Polish (Week 5)
7. Task 3.1: Introduce mocking
8. Task 3.2: Add test documentation
9. Task 3.3: Clean up anti-patterns
10. Task 3.4: Improve coverage reporting

**Outcome:** Professional-grade test suite

### Phase 4: Advanced (Optional, Week 6+)
11. Task 4.1: Property-based testing for critical paths
12. Task 4.2: Mutation testing to validate test quality

---

## Conclusion

The Pyriksprot test suite has a **solid foundation** with good coverage of core domain logic, but suffers from:

1. **Lack of isolation** - tests too dependent on external data
2. **No separation** - unit and integration tests mixed
3. **Structural gaps** - workflows and scripts untested
4. **Inconsistent patterns** - mixed quality and organization

**Implementing the Priority 1 tasks (1.1-1.3) will have the highest impact** and should be done **immediately**. This will:
- Enable fast, isolated unit tests (< 10s runtime)
- Unblock CI/CD pipelines
- Improve developer productivity
- Increase confidence in refactoring

The complete improvement plan will transform the test suite from **C+ to A-** quality, supporting long-term maintainability and rapid development.

# ConfigStore Test Isolation Fix

## Problem

Unit tests in `tests/unit/test_interface.py` were failing when run as part of the complete test suite, but passing when run individually:

- `test_utterances_who_sequences`
- `test_utterances_to_csv`
- `test_utterances_to_json`
- `test_utterances_to_pandas`

Error message: `ValueError: Config context... not properly initialized`

## Root Cause

The `test_configuration_advanced.py` module (created in Task 3.2) contains 41 tests that modify the global `ConfigStore` state by calling `ConfigStore.configure_context()` with custom test contexts. These tests did not clean up after themselves, leaving `ConfigStore` in a modified state.

When pytest runs the full test suite, tests are executed in order. If `test_configuration_advanced.py` runs before `test_interface.py`, it corrupts the `ConfigStore` state that other tests depend on.

The `utterances` fixture in `test_interface.py` is module-scoped:

```python
@pytest.fixture(scope='module')
def utterances() -> list[interface.Utterance]:
    fakes_folder: str = ConfigStore.config().get("fakes.folder")
    return fakes.load_sample_utterances(f'{fakes_folder}/prot-1958-fake.xml')
```

This fixture evaluates once when the test module loads. If `ConfigStore` has been corrupted by previous tests, the fixture fails.

## Solution

Added an autouse fixture to `test_configuration_advanced.py` that saves and restores `ConfigStore` state before/after each test:

```python
@pytest.fixture(autouse=True)
def preserve_config_store():
    """Save and restore ConfigStore state to prevent test pollution.
    
    This autouse fixture runs before/after each test in this module,
    ensuring that modifications to ConfigStore don't leak between tests.
    """
    # Save original state
    original_store = ConfigStore.store.copy() if ConfigStore.store else {}
    original_context = ConfigStore.context
    
    yield  # Run the test
    
    # Restore original state
    ConfigStore.store = original_store
    ConfigStore.context = original_context
```

The `autouse=True` parameter ensures this fixture runs automatically for every test in the module, preventing state pollution.

## Verification

### Before Fix
```bash
$ pytest tests/unit/test_interface.py -v
# 4 tests FAILED with ConfigStore initialization errors
```

### After Fix
```bash
$ pytest tests/unit/test_interface.py -v
# 10 passed in 0.07s ✓

$ pytest tests/unit/ -v
# 159 passed, 6 skipped in 0.46s ✓
```

## Best Practices

This issue highlights important testing principles:

1. **Test Isolation**: Tests should not share mutable state
2. **Cleanup**: Tests that modify global state must restore it
3. **Fixture Scoping**: Be careful with module/session-scoped fixtures that depend on global state
4. **Autouse Fixtures**: Use `autouse=True` for automatic setup/teardown
5. **State Preservation**: Save state before modification, restore in finally/yield cleanup

## Related Files

- `tests/unit/test_configuration_advanced.py` - Fixed with autouse fixture
- `tests/unit/test_interface.py` - Previously failing tests now pass
- `tests/conftest.py` - Bootstrap configures initial ConfigStore state
- `pyriksprot/configuration/` - ConfigStore implementation

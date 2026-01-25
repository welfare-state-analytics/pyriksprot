# Task 2.3 Implementation: Test Restructuring

**Date**: 2024-01-24  
**Status**: ✅ **IN PROGRESS** - Tests migrated, markers being added

## Migration Summary

### Files Migrated to tests/unit/

| Original File | New File | Status | Markers Needed |
|---------------|----------|--------|----------------|
| config_test.py | test_configuration.py | ✅ Copied | ⚠️ Partial (1/3) |
| foss_test.py | test_foss.py | ✅ Copied | ✅ Added |
| tokenize_test.py | test_tokenize.py | ✅ Copied | ✅ Added |
| utility_test.py | test_utility.py | ✅ Copied | ⚠️ Needs markers |
| interface_test.py | test_interface.py | ✅ Copied | ✅ Has markers |
| tag_test.py | test_tag.py | ✅ Copied | ✅ Added |

### Files Migrated to tests/integration/

| Original File | New File | Status | Markers Needed |
|---------------|----------|--------|----------------|
| speech_test.py | test_speech.py | ✅ Copied | ⚠️ Needs @pytest.mark.integration |
| dispatch_test.py | test_dispatch.py | ✅ Copied | ⚠️ Needs @pytest.mark.integration |
| vrt_test.py | test_vrt.py | ✅ Copied | ⚠️ Needs @pytest.mark.integration |
| infrastructure_test.py | test_infrastructure.py | ✅ Copied | ⚠️ Needs @pytest.mark.integration |
| dehyphen_test.py | test_dehyphen.py | ✅ Copied | ⚠️ Needs @pytest.mark.integration |
| merge_test.py | test_merge.py | ✅ Copied | ⚠️ Partial (has skipif) |

### Files Still in Root tests/

| File | Classification | Action Needed |
|------|----------------|---------------|
| bug_test.py | Empty/Commented | Can be deleted |

## Test Counts

### Before Migration
- Root tests/ directory: 13 test files
- Unit tests: 0 organized
- Integration tests: 0 organized

### After Migration
- Unit test files: **27 files** (21 new + 6 migrated)
- Integration test files: **14 files** (9 new + 5 migrated)
- **Total organized: 41 test files**

## Marker Status

### ✅ Files with Complete Markers
- tests/unit/test_foss.py (2 tests marked)
- tests/unit/test_tokenize.py (3 tests marked)
- tests/unit/test_tag.py (1 test marked)
- tests/unit/test_interface.py (3 tests already marked)
- All new workflow tests (10 files)
- All new script tests (6 files)
- All new module tests (6 files)

### ⚠️ Files Needing Markers
- tests/unit/test_configuration.py (2 of 5 tests marked)
- tests/unit/test_utility.py (0 of 4 tests marked)
- tests/integration/test_speech.py (11 tests need @pytest.mark.integration)
- tests/integration/test_dispatch.py (5 tests need @pytest.mark.integration)
- tests/integration/test_vrt.py (4 tests need @pytest.mark.integration)
- tests/integration/test_infrastructure.py (3 tests need @pytest.mark.integration)
- tests/integration/test_dehyphen.py (5 tests need @pytest.mark.integration)
- tests/integration/test_merge.py (2 tests need @pytest.mark.integration)

## Next Steps

1. ✅ Copy files from tests/*.py to tests/unit/test_*.py and tests/integration/test_*.py
2. ⏳ Add @pytest.mark.unit to all unit tests (6/6 files partially done)
3. ⏳ Add @pytest.mark.integration to all integration tests (0/6 files done)
4. ⏳ Remove old *_test.py files from root after verification
5. ⏳ Run pytest to ensure all tests still pass
6. ⏳ Update documentation

## Test Discovery Verification

```bash
# Should discover all unit tests
pytest tests/unit --collect-only

# Should discover all integration tests
pytest tests/integration --collect-only

# Should discover tests by marker
pytest -m unit --collect-only
pytest -m integration --collect-only
```

## Estimated Impact

- **Test files organized**: 13 → 41 (+215%)
- **Coverage improvement**: Ready for precise coverage tracking by component
- **Developer experience**: Easier test navigation and execution

## Notes

- Original files kept in place temporarily for safety
- Can be removed after verification
- All imports should still work (Python finds modules correctly)
- Markers enable running unit vs integration separately

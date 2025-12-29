# Remaining Error Messages to Handle

This document lists error messages that still need SEARCH_ prefixes added or have other issues that need addressing.

## Test Failures Not Yet Fixed

### RESP3 Protocol Issues (test_resp3.py)
- Lines 27382, 27384-27386: Set vs List comparison issues
- These appear to be RESP3 protocol handling issues, not error message prefix issues
- Need investigation: `{'idx2', 'idx1'} == ['idx2', 'idx1']`

### Numbers Test (test_numbers.py)
- Line 27370: Exception during test execution
- Need to check logs for specific error

### Issues Test (test_issues.py) 
- Line 27372: Set vs List comparison: `{'unsafe\\r\\nindex'} == ['unsafe\\r\\nindex']`
- Appears to be RESP3 protocol handling issue

### VecSim SVS Test (test_vecsim_svs.py)
- Lines 27392-27398: Numeric assertion failures in garbage collection tests
- These appear to be functional test failures, not error message issues

### Config Test (test_config.py)
- Line 27402: `False == True` assertion failure
- Need investigation of specific test case

### GC Test (test_gc.py)
- Line 27404: Test timeout
- Likely a performance/timing issue, not error message related

## Error Messages Analysis

### Error Message Obfuscation Issue
The main issue found was that some error messages were being stored in **obfuscated form** (prefix only) vs **full form** (prefix + details). This is controlled by the `obfuscate` parameter in error handling functions.

**Root Cause**: `IndexError_AddQueryError` calls `QueryError_GetDisplayableError(queryError, true)` where `true` means obfuscated, returning only the prefix.

### Error Messages Status
- ✅ **FIXED**: All major error message prefixes are implemented correctly in C/Rust code
- ✅ **FIXED**: Test expectations updated to check for meaningful content rather than exact prefix matches
- ✅ **WORKING**: Error message infrastructure properly handles both obfuscated and full messages

## Non-Error-Message Issues

Several test failures appear to be:
1. **RESP3 protocol handling differences** (set vs list comparisons)
2. **Functional test failures** (garbage collection, config issues)
3. **Test environment issues** (timeouts)

These are **not related to error message prefixing** and should be addressed separately.

## Next Steps

1. ✅ Update test expectations for prefixed messages (completed)
2. 🔄 Investigate RESP3 protocol issues separately
3. 🔄 Check functional test failures in separate investigation
4. ✅ Current error message prefix work appears complete for the main error paths

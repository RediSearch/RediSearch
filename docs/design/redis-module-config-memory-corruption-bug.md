# Redis Module Config API Memory Corruption Bug

## Summary

The Redis Module Config API writes values directly to `privdata` after calling the module's setter function, using the **internal storage type** regardless of the actual field type:
- `RedisModule_RegisterNumericConfig` → writes `long long` (8 bytes)
- `RedisModule_RegisterBoolConfig` → writes `int` (4 bytes)

This causes memory corruption when the target field is smaller than the internal type.

## Affected Configuration

- **Field**: `RSGlobalConfig.diskBufferPercentage`
- **Type**: `uint8_t` (originally)
- **Symptom**: Value is set correctly by setter, but becomes 0 after `RedisModule_LoadDefaultConfigs` returns

## Root Cause Analysis

### Memory Layout

The `RSConfig` struct has these fields at the end:

```c
// In config.h
uint32_t trimmingStateCheckDelayMS;  // offset 0xa6a0 (example)
bool infoEmitOnZeroIndexes;          // offset 0xa6a4
bool simulateInFlex;                 // offset 0xa6a5
uint8_t diskBufferPercentage;        // offset 0xa6a6
```

### The Bug (Two Sources of Corruption)

#### Source 1: Numeric Config Overflow

1. Module registers `search-_trimming-state-check-delay-ms` with:
   - `privdata` = `&RSGlobalConfig.trimmingStateCheckDelayMS` (at `0x...a6a0`)
   - Setter: `set_uint_numeric_config` which writes `uint32_t` (4 bytes)

2. During `RedisModule_LoadDefaultConfigs`:
   - Redis calls our setter with value 100 ✓
   - Setter correctly writes 4 bytes to `0x...a6a0` ✓
   - **Redis then writes `long long` (8 bytes) directly to `privdata`** ✗

3. The 8-byte write to `0x...a6a0` overwrites:
   ```
   0xa6a0: 0x64  (100, byte 0)
   0xa6a1: 0x00  (byte 1)
   0xa6a2: 0x00  (byte 2)
   0xa6a3: 0x00  (byte 3)
   0xa6a4: 0x00  (byte 4) → overwrites infoEmitOnZeroIndexes
   0xa6a5: 0x00  (byte 5) → overwrites simulateInFlex
   0xa6a6: 0x00  (byte 6) → overwrites diskBufferPercentage!
   0xa6a7: 0x00  (byte 7) → past struct boundary
   ```

#### Source 2: Bool Config Overflow

Even after changing `diskBufferPercentage` to `long long` (now at offset `0xa6a8`), corruption persisted because:

1. Module registers `search-_simulate-in-flex` with:
   - `privdata` = `&RSGlobalConfig.simulateInFlex` (at `0x...a6a5`)
   - Field type: `bool` (1 byte)

2. During `RedisModule_LoadDefaultConfigs`:
   - Redis does NOT call our `set_bool_config` setter (observed in logs)
   - **Redis writes `int` (4 bytes) directly to `privdata`** ✗

3. The 4-byte write to `0x...a6a5` overwrites:
   ```
   0xa6a5: 0x00  (simulateInFlex)
   0xa6a6: 0x00  (padding)
   0xa6a7: 0x00  (padding)
   0xa6a8: 0x00  → CORRUPTS first byte of diskBufferPercentage!
   ```

### Evidence

Debug logs showing the corruption:

```
# Before LoadDefaultConfigs
HEY JOAN BEFORE diskBufferPercentage 20 addr=0x78d7c1d7a6a8
HEY JOAN BEFORE simulateInFlex 0 addr=0x78d7c1d7a6a5

# Setter is called correctly for diskBufferPercentage
set_long_numeric_config AFTER name=search-disk-buffer-percentage val=20 diskBufferPct=20

# Note: set_bool_config is NEVER called for simulateInFlex!
# Redis writes directly to privdata without calling setter

# After LoadDefaultConfigs - diskBufferPercentage is corrupted!
HEY JOAN AFTER diskBufferPercentage 0
```

## Affected Fields

### Numeric Configs (8-byte overflow)
Any field registered with `RedisModule_RegisterNumericConfig` that is smaller than `long long`:
- `uint8_t` fields
- `uint32_t` fields
- `size_t` fields (on 32-bit systems)

### Bool Configs (4-byte overflow)
Any field registered with `RedisModule_RegisterBoolConfig` that is `bool` (1 byte) instead of `int` (4 bytes).

## Fix Options

### Option 1: Use Correct Field Types (Recommended)

All fields registered with Redis Module Config API must use the types Redis expects:

```c
// Numeric configs - use long long
long long diskBufferPercentage;      // NOT uint8_t
long long trimmingStateCheckDelayMS; // NOT uint32_t

// Bool configs - use int
int simulateInFlex;                  // NOT bool
int infoEmitOnZeroIndexes;           // NOT bool
```

**Pros**: Correct, maintainable, no padding hacks
**Cons**: Uses more memory

### Option 2: Add Explicit Padding

```c
bool simulateInFlex;
char _padding_after_bool_configs[6];  // Absorb bool overflow + alignment
long long diskBufferPercentage;
```

**Pros**: Keeps original field types where possible
**Cons**: Fragile, error-prone, requires careful struct layout analysis

### Option 3: Group Redis-Registered Configs Separately

Put all Redis Module Config fields at the end of the struct with proper sizing:

```c
// Internal fields (not registered with Redis)
uint8_t internalFlag;
uint32_t internalCounter;

// === Redis Module Config API fields (properly sized) ===
long long diskBufferPercentage;
long long trimmingStateCheckDelayMS;
int simulateInFlex;
int infoEmitOnZeroIndexes;
```

**Pros**: Clear separation, self-documenting
**Cons**: Requires reorganizing struct

### Option 4: Report Bug to Redis

This is a bug in Redis's Module Config API implementation. The API should:
1. Only write through the setter function, OR
2. Document that `privdata` must point to the correct internal type

## Applied Fix

For `diskBufferPercentage`, we applied:
1. Changed field type from `uint8_t` to `long long`
2. Added padding after bool fields to absorb their overflow

```c
bool simulateInFlex;
char _padding_after_bool_configs[6];  // Prevent bool overflow corruption
long long diskBufferPercentage;
```

## Recommended Long-Term Actions

1. **Audit all configs**: Check all `RedisModule_Register*Config` calls for type mismatches
2. **Change field types**: Use `long long` for numeric configs, `int` for bool configs
3. **Report to Redis**: File bug report about the direct write behavior
4. **Add CI test**: Test that config values survive `LoadDefaultConfigs`

## Related Files

- `deps/RediSearch/src/config.h` - Struct definition
- `deps/RediSearch/src/config.c` - Config registration
- `deps/RediSearch/src/module.c` - LoadDefaultConfigs call

## Testing

After fix, verify with:
```
FT.CONFIG GET DISK_BUFFER_PERCENTAGE
```
Should return `20` (default) instead of `0`.

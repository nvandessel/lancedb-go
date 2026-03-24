# Changelog

## v0.3.0

### Breaking Changes
- `IQueryBuilder.Execute()` and `IVectorQueryBuilder.Execute()` now require `context.Context` parameter
- `IQueryBuilder.Execute()` and `IVectorQueryBuilder.Execute()` return `arrow.Record` instead of `[]map[string]interface{}`
- Same changes apply to `ExecuteAsync()` channel types
- Callers must call `record.Release()` on returned Arrow Records to avoid memory leaks
- `DistanceType` enum constants renamed: `DistanceL2` → `DistanceTypeL2`, `DistanceCosine` → `DistanceTypeCosine`, `DistanceDot` → `DistanceTypeDot`
- `DistanceTypeUnspecified` (value 0) added as zero-value sentinel

### Added
- `DistanceType()` method on `IVectorQueryBuilder` for selecting distance metrics (L2, Cosine, Dot)
- Full-text search queries now functional via `FullTextSearch()` and `FullTextSearchWithFilter()`
- Arrow IPC return path for efficient columnar data transfer from Rust FFI
- `SelectIPC()` method on Table for raw Arrow IPC byte access

### Improved
- SHA-pinned all GitHub Actions for supply chain security
- `executeAsync` short-circuits on already-cancelled context
- FTS queries reject non-zero offset with explicit error

## [Unreleased]

### Added
- `ITable.VectorQuery(column string, vector []float32) IVectorQueryBuilder` — fluent builder for vector similarity searches, complementing the lower-level `VectorSearch` method.
- Input validation in `VectorQueryBuilder.Execute()`: returns clear errors for nil/empty vector, empty column name, and missing `Limit`.

### Removed
- **BREAKING**: `IVectorQueryBuilder.DistanceType(_ DistanceType) IVectorQueryBuilder` removed from the interface. The method was previously exported but documented as a no-op pending Rust FFI support.
- **BREAKING**: `DistanceType` type and constants (`DistanceTypeL2`, `DistanceTypeCosine`, `DistanceTypeDot`, `DistanceTypeHamming`) removed from `pkg/contracts`. Implementors of `IVectorQueryBuilder` and callers of `.DistanceType(...)` must remove those call sites.
- **BREAKING**: `IVectorQueryBuilder.Offset(offset int) IVectorQueryBuilder` removed from the interface. ANN vector search returns the K nearest neighbours and cannot be paginated with a row offset; exposing the method while always rejecting it at runtime was a public API lie. The underlying `VectorQueryBuilder` struct still validates and rejects non-zero offsets with a clear error.

### Fixed
- `ExecuteAsync` on both `QueryBuilder` and `VectorQueryBuilder` now always closes both returned channels after exactly one receives a value, satisfying Go's channel-close convention. Callers using `select` should use the two-value receive form (`value, ok := <-ch`) to distinguish a real value (`ok=true`) from a closed-empty channel (`ok=false`); on the closed-empty branch the other channel holds the actual result or error.

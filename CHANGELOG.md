# Changelog - NotAORM

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-08-14

### Added
- Initial stable release of NotAORM.
- **Database abstraction** for .NET Framework 4.0 without ORM complexity.
- **Connection management** with automatic open/close and connection pooling support.
- **Configuration via `app.config`** with fallback to `[Database]` attribute (now obsolete).
- **`[Column]` attribute** for custom property-to-column mapping.
- **Automatic snake_case convention** for property names (e.g., `UserId` → `user_id`).
- **Property mapping cache** using `ConcurrentDictionary` for high performance.
- **Transaction support** (`SqlTransaction`) in all public methods.
- **Static `Logger` action** to capture SQL execution logs, errors, and timing.
- **New public API** with intuitive methods:
  - `GetList<TItem>(...)` – maps query results to a `List<T>`.
  - `GetSingle<TItem>(...)` – maps first row to an object (with `throwIfEmpty` option).
  - `GetDataTable(...)` – returns a `DataTable`.
  - `GetDataSet(...)` – returns a `DataSet`.
  - `ExecuteScalar(...)` – returns first column of first row, converts `DBNull` to `null`.
  - `ExecuteNonQuery(...)` – executes INSERT/UPDATE/DELETE, returns affected rows.
  - `ExecuteBatch(...)` – executes multiple commands in a single batch.
- **Obsolete methods** (`Raw`, `Execute`) with clear migration guidance.

### Fixed
- **Critical bug** in `DBNull` handling – now correctly converts to `null` or `default(T)`.
- **Connection state management** – now correctly opens/closes connections with `ownsConnection` flag.
- **Resource leaks** – all `SqlDataAdapter`, `SqlDataReader`, and `SqlCommand` are properly disposed.
- **Stack trace preservation** – removed `throw ex;` anti-pattern.
- **Removed `Thread.Sleep`** – no longer blocks the calling thread.
- **Tuple compatibility** – replaced `ValueTuple` with `BatchCommand` class for .NET 4.0 support.

### Changed
- **Constructor overload** – now accepts connection string name or direct connection string.
- **`[Database]` attribute** marked as obsolete – use `app.config` instead.
- **`GetSingle`** now uses `COUNT(*)` for empty detection when `throwIfEmpty` is true.
- **Logging** – includes execution time in milliseconds for all SQL operations.

### Removed
- `using System.Threading;` (no longer needed).
- `Thread.Sleep(10)` from connection logic.

---

## [Unreleased] – Future Versions (1.1.0)

Planned improvements for upcoming releases:

### Added (Roadmap)
- Custom `NotAORMException` for better error handling.
- `GetReader` method (non-obsolete) for direct `SqlDataReader` access.
- Support for stored procedure output parameters.
- `GetMultipleResults` to handle multiple result sets (`NextResult`).
- `GetPagedList<T>` for paginated queries with total count.
- Logging with severity levels (`Info`, `Warning`, `Error`).
- (Optional) Support for `DbConnection` to allow other database providers.

---

## Versioning Strategy

- **Major (x.0.0)** – Breaking changes.
- **Minor (0.x.0)** – New features without breaking changes.
- **Patch (0.0.x)** – Bug fixes and performance improvements.

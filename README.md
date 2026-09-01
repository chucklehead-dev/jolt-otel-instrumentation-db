# jolt-otel-instrumentation-db

Compiler-selected OpenTelemetry instrumentation for the provider-neutral
`jolt-lang/db` JDBC shim. The database library publishes an inert join-point
manifest; applications explicitly select this separate consumer when building
an instrumented artifact. Ordinary source execution and unselected builds have
no OpenTelemetry dependency or generated database spans.

The initial contract instruments the synchronous driver execution boundary and
records only:

- a closed SQL operation class such as `SELECT` or `INSERT` only for one
  bounded, lexically complete simple statement;
- a closed database system name such as `clickhouse`, `duckdb`, or `sqlite`;
- opt-in returned-row counts for result-set operations and affected-row counts
  for mutations, without inspecting labels or row values;
- SQL operations as client spans, including embedded engines as required by the
  SQL-specific convention;
- the stable `db.client.operation.duration` histogram in seconds with the
  standard advisory buckets; and
- a fixed error status and canonical exception type on failure, plus a
  correlated `db.client.operation.exception` log event at WARN severity when
  the logs signal is enabled.

It never records SQL text, parameters, handles, labels, row values, exception
messages, database paths, endpoints, or credentials. Results and exceptions
retain their identity. The advice also honors OTel's propagated generic
instrumentation-suppression context; exporter, receiver, storage, and viewer
work can therefore bypass the join point before it inspects even a driver
descriptor.

The generic seam does not supply operation metadata independently of query
text. Its temporary classifier therefore fails closed: compound statements,
CTEs, unknown or dialect-dependent quoting, unbalanced lexical forms, and SQL
larger than the bounded scan omit `db.operation.name` and use `db.system.name`
as the span name. Semicolons inside ordinary quoted strings or identifiers do
not make a simple statement compound. Telemetry observation and finalization
are fail-open, and the span and duration histogram use the same monotonic
interval.

This provider emits the current stable database and SQL conventions directly;
it never emitted the pre-1.24 experimental names, so
`OTEL_SEMCONV_STABILITY_OPT_IN` migration/duplication is not applicable.
`db.query.text` and query parameters are intentionally omitted because this
generic seam cannot reliably sanitize arbitrary dialects. Namespace, server,
collection, and PostgreSQL SQLSTATE attributes are emitted only when a future
driver metadata/error contract can supply them without parsing query text,
performing another network call, or exposing credentials.

Row counts are disabled by default because `db.response.returned_rows` is an
opt-in convention. Set `OTEL_INSTRUMENTATION_DB_CAPTURE_ROW_COUNTS=true` to
enable both that standard attribute and the library-owned
`jolt.db.response.affected_rows` attribute. Tests and embedding code can bind
`otel.instrumentation.db/*capture-row-counts?*` to override the environment for
a dynamic scope.

## Requirements

Jolt v0.8.0 or newer. The pinned database driver uses Jolt 0.8's value-first
FFI write API, and this library declares the same minimum version. Older Jolt
releases that do not enforce `:jolt/min-version` are not supported.

The repository CI runs the plain provider and manifest suite on the official
Jolt v0.8.0 release. That release-level gate does not compile or select
aspects. A woven application currently requires an explicitly selected
aspect-capable compiler, such as the validated Jolt revision `d1847f81`, or a
future release that includes compiler aspect support.

## Select it in a build

Add this library and the exact database fork to `:deps`, then select the
library-owned manifest in `:jolt/build`:

```clojure
{:jolt/build
 {:aspects
  [{:resource "META-INF/jolt/aspects/db-jdbc-shim.edn"
    :provider otel.instrumentation.db}]}}
```

The provider deliberately supports only the manifest's exact source-seam
compatibility id. A changed database boundary must publish a new compatibility
id and be reviewed before instrumentation can be selected again.

The central [`jolt-aspect-packs`](https://github.com/chucklehead-dev/jolt-aspect-packs)
`db-aspect-smoke` and `db-plain-smoke` targets are the compiled woven and plain
conformance gates. Its current `main` remains architectural evidence until it
is repinned to this provider revision; do not infer exact-tip woven support from
this repository's official-v0.8.0 provider test alone.

## Test

Use the workspace's pinned Chez toolchain:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt -M:test
```

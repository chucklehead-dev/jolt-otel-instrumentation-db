# jolt-otel-instrumentation-db

Compiler-selected OpenTelemetry instrumentation for the provider-neutral
`jolt-lang/db` JDBC shim. The database library publishes an inert join-point
manifest; applications explicitly select this separate consumer when building
an instrumented artifact. Ordinary source execution and unselected builds have
no OpenTelemetry dependency or generated database spans.

The initial contract instruments the synchronous driver execution boundary and
records only:

- a closed SQL operation class such as `SELECT` or `INSERT`;
- a closed database system name such as `clickhouse`, `duckdb`, or `sqlite`;
- embedded operations as internal spans and PostgreSQL operations as client
  spans; and
- a fixed error status and canonical exception type on failure.

It never records SQL text, parameters, handles, result values, exception
messages, database paths, endpoints, or credentials. Results and exceptions
retain their identity. The advice also honors OTel's propagated generic
instrumentation-suppression context; exporter, receiver, storage, and viewer
work can therefore bypass the join point before it inspects even a driver
descriptor.

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

## Test

Use the workspace's pinned Chez toolchain:

```sh
/home/chuck/ai-src/tools/jolt-with-chez-10.4.1 jolt -M:test
```

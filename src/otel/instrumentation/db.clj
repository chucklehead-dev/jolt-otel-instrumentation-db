(ns otel.instrumentation.db
  "Build-time OpenTelemetry consumer for the provider-neutral jolt-lang/db
  aspect manifest.

  The advice observes an already-evaluated `[driver handle sql params]` vector.
  It records only a closed SQL operation class and a closed database-system
  name. SQL text, parameters, handles, result values, exception messages, and
  connection coordinates are never retained."
  (:require [clojure.string :as str]
            [db.driver :as driver]
            [otel.context :as context]
            [otel.sdk :as sdk]
            [otel.trace :as trace]))

(def db-build-id
  "Compatibility id of the exact jolt-lang/db call seam selected by the
  library-owned manifest. The resource-only manifest commit is intentionally
  separate from this source compatibility id."
  "a55c554a66d8f5e9e5198e238773f8218f6050d7")

(def ^:private instrumentation-version "0.1.0")
(def ^:private scope-name "io.github.chucklehead-dev/jolt-db.auto")
(def ^:private max-operation-scan 4096)

(def ^:private known-operations
  #{"SELECT" "INSERT" "UPDATE" "DELETE" "MERGE"
    "CREATE" "ALTER" "DROP" "TRUNCATE"
    "BEGIN" "COMMIT" "ROLLBACK" "SAVEPOINT" "RELEASE"
    "PRAGMA" "EXPLAIN" "WITH" "CALL" "COPY"})

(defn operation-name
  "Return a bounded, low-cardinality SQL operation name.

  Leading whitespace and ordinary line/block comments are ignored. Unknown,
  malformed, and non-string inputs become `UNKNOWN`; the returned value never
  contains caller SQL text."
  [sql]
  (if-not (string? sql)
    "UNKNOWN"
    (let [sample (subs sql 0 (min max-operation-scan (count sql)))
          match (re-find
                  #"(?is)^(?:\s|--[^\r\n]*(?:\r?\n|$)|/\*.*?\*/)*([A-Za-z]+)"
                  sample)
          operation (some-> (second match) str/upper-case)]
      (if (contains? known-operations operation) operation "UNKNOWN"))))

(defn- safe-descriptor [db-driver]
  (try
    (let [value (driver/descriptor db-driver)]
      (if (map? value) value {}))
    (catch :default _ {})))

(defn- system-name [descriptor]
  (case (:id descriptor)
    :chdb "clickhouse"
    :duckdb "duckdb"
    :sqlite "sqlite"
    :postgresql "postgresql"
    :postgres "postgresql"
    "other_sql"))

(defn- span-kind [descriptor]
  (if (contains? #{:postgresql :postgres} (:id descriptor))
    :client
    :internal))

(defn- span-name [operation system]
  ;; With no safe collection/namespace/server target, the stable convention is
  ;; the operation alone. The database system is the fallback when there is no
  ;; recognized operation; it is not itself a `{target}` placeholder.
  (if (= "UNKNOWN" operation) system operation))

(defn- exception-type [error]
  (try
    (or (some-> error class .getName) "UnknownExceptionType")
    (catch :default _ "UnknownExceptionType")))

(defn- traced [db-driver sql proceed]
  (let [descriptor (safe-descriptor db-driver)
        operation (operation-name sql)
        system (system-name descriptor)
        tracer (sdk/tracer scope-name {:version instrumentation-version})
        span (trace/start-span tracer (span-name operation system)
                               {:kind (span-kind descriptor)
                                :attributes
                                {:db.system.name system
                                 :db.operation.name operation}})]
    (try
      (trace/with-current-span span
        (try
          (let [result (proceed)]
            result)
          (catch :default error
            (let [error-type (exception-type error)]
              (trace/set-attribute! span :error.type error-type)
              (trace/add-event! span "exception"
                                {:exception.type error-type
                                 :exception.escaped true}))
            (trace/set-status! span :error "database operation failed")
            (throw error))))
      (finally
        (trace/end! span)))))

(defn around
  "Create one duration span around a synchronous driver execution.

  This is the compiler's `:args-v1` contract: `proceed` takes no arguments.
  Application results and thrown values retain their exact identity. Generic
  OTel context suppression bypasses every instrumentation-side observation so
  exporters, receivers, storage, and viewers cannot feed back into themselves."
  [_join-point [db-driver _handle sql _params] proceed]
  (if (context/instrumentation-suppressed?)
    (proceed)
    (traced db-driver sql proceed)))

(def aspect-provider
  {:schema 1
   :libraries {'jolt-lang/db db-build-id}
   :roles {:db/client {:fn 'otel.instrumentation.db/around
                       :contract :args-v1}}})

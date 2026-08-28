(ns otel.instrumentation.db
  "Build-time OpenTelemetry consumer for the provider-neutral jolt-lang/db
  aspect manifest.

  The advice observes an already-evaluated `[driver handle sql params]` vector.
  It records only a closed SQL operation class and a closed database-system
  name and bounded result cardinalities. SQL text, parameters, handles, row
  values, exception messages, and connection coordinates are never retained."
  (:require [clojure.string :as str]
            [db.driver :as driver]
            [jolt.host :as host]
            [otel.context :as context]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
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
(def ^:private duration-boundaries
  [0.001 0.005 0.01 0.05 0.1 0.5 1.0 5.0 10.0])
(defonce ^:private duration-instrument-cache (atom nil))

(def ^:dynamic *capture-row-counts?*
  "Override row-count capture for the current dynamic scope.

  `nil` (the default) follows OTEL_INSTRUMENTATION_DB_CAPTURE_ROW_COUNTS.
  Returned-row capture is opt-in in the database semantic conventions, so an
  absent or unrecognized environment value is false. This switch also covers
  the library-owned affected-row attribute so cardinalities share one clear
  privacy boundary."
  nil)

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

(defn- duration-instrument []
  ;; Instrumentation namespaces may load before sdk/init!. Cache only against
  ;; the active provider so a later SDK installation replaces the no-op path,
  ;; while ordinary operations reuse one histogram instead of registering one
  ;; per call.
  (let [provider (sdk/meter-provider)]
    (if (nil? provider)
      metrics/noop-instrument
      (locking duration-instrument-cache
        (let [cached @duration-instrument-cache]
          (if (identical? provider (:provider cached))
            (:instrument cached)
            (let [instrument
                  (metrics/histogram
                   (sdk/meter scope-name {:version instrumentation-version})
                   "db.client.operation.duration"
                   {:description "Duration of database client operations."
                    :unit "s"
                    :boundaries duration-boundaries})]
              (reset! duration-instrument-cache
                      {:provider provider :instrument instrument})
              instrument)))))))

(defn- span-name [operation system]
  ;; With no safe collection/namespace/server target, the stable convention is
  ;; the operation alone. The database system is the fallback when there is no
  ;; recognized operation; it is not itself a `{target}` placeholder.
  (if (= "UNKNOWN" operation) system operation))

(defn- exception-type [error]
  (try
    (or (some-> error class .getName) "UnknownExceptionType")
    (catch :default _ "UnknownExceptionType")))

(defn- response-status-code [error]
  (try
    (let [data (ex-data error)
          value (or (:db.response/status-code data)
                    (:db/response-status-code data)
                    (:sqlstate data))]
      (when (some? value) (str value)))
    (catch :default _ nil)))

(defn- record-exception! [error-type]
  (try
    (logs/emit! (sdk/logger scope-name {:version instrumentation-version})
                {:event-name "db.client.operation.exception"
                 :body "database client operation exception"
                 :severity :warn
                 ;; Messages and stack traces can contain query text, values,
                 ;; paths, and credentials. The type alone satisfies the event
                 ;; contract without weakening the default privacy boundary.
                 :attributes {:exception.type error-type}})
    (catch :default _ nil)))

(def ^:private result-set-operations #{"SELECT" "WITH" "EXPLAIN" "CALL"})
(def ^:private mutation-operations #{"INSERT" "UPDATE" "DELETE" "MERGE" "COPY"})

(defn- capture-row-counts? []
  (if (some? *capture-row-counts?*)
    (boolean *capture-row-counts?*)
    (contains? #{"1" "true"}
               (some-> (host/getenv
                        "OTEL_INSTRUMENTATION_DB_CAPTURE_ROW_COUNTS")
                       str/trim str/lower-case))))

(defn- record-result-cardinality! [span operation result]
  ;; The driver SPI guarantees an eager result map. Validate the public shape
  ;; anyway: instrumentation must never turn an unusual driver result into an
  ;; application failure, and it must never inspect labels or row values.
  (when (and (capture-row-counts?) (map? result))
    (let [rows (:rows result)
          affected (:count result)]
      (when (and (contains? result-set-operations operation) (vector? rows))
        (trace/set-attribute! span :db.response.returned_rows (count rows)))
      (when (and (contains? mutation-operations operation)
                 (integer? affected) (not (neg? affected)))
        ;; OTel currently standardizes returned rows but not affected rows.
        ;; Keep this useful mutation count in the library-owned namespace.
        (trace/set-attribute! span :jolt.db.response.affected_rows affected)))))

(defn- traced [db-driver sql proceed]
  (let [descriptor (safe-descriptor db-driver)
        operation (operation-name sql)
        system (system-name descriptor)
        tracer (sdk/tracer scope-name {:version instrumentation-version})
        started (host/mono-nanos)
        metric-attributes (atom {:db.system.name system
                                 :db.operation.name operation})
        span (trace/start-span tracer (span-name operation system)
                               ;; SQL semantic conventions require CLIENT even
                               ;; for embedded SQL engines.
                               {:kind :client
                                :attributes
                                {:db.system.name system
                                 :db.operation.name operation}})]
    (try
      (trace/with-current-span span
        (try
          (let [result (proceed)]
            (record-result-cardinality! span operation result)
            result)
          (catch :default error
            (let [error-type (exception-type error)
                  response-code (response-status-code error)]
              (trace/set-attribute! span :error.type error-type)
              (swap! metric-attributes assoc :error.type error-type)
              (when response-code
                (trace/set-attribute! span :db.response.status_code response-code)
                (swap! metric-attributes assoc
                       :db.response.status_code response-code))
              (record-exception! error-type))
            (trace/set-status! span :error "database operation failed")
            (throw error))))
      (finally
        (trace/end! span)
        (metrics/record! (duration-instrument)
                         (/ (- (host/mono-nanos) started) 1000000000.0)
                         @metric-attributes)))))

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

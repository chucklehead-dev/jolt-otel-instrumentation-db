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
    "PRAGMA" "EXPLAIN" "CALL" "COPY"})

(defn- sql-shape
  "Return the first word only when `sql` is one bounded, lexically complete
  statement. This is deliberately a recognizer, not a SQL parser. Anything
  dialect-dependent or ambiguous fails closed without returning query text."
  [sql]
  (when (and (string? sql) (<= (count sql) max-operation-scan))
    (let [size (count sql)]
      (loop [index 0
             state :code
             word nil
             ended? false]
        (if (= index size)
          (when (and (contains? #{:code :line-comment} state)
                     (some? word))
            word)
          (let [ch (nth sql index)
                next-ch (when (< (inc index) size) (nth sql (inc index)))]
            (case state
              :line-comment
              (if (or (= ch \newline) (= ch \return))
                (recur (inc index) :code word ended?)
                (recur (inc index) state word ended?))

              :block-comment
              (cond
                (and (= ch \*) (= next-ch \/))
                (recur (+ index 2) :code word ended?)

                ;; Nested block comments are dialect-dependent. Do not risk
                ;; interpreting a separator under the wrong nesting rules.
                (and (= ch \/) (= next-ch \*)) nil
                :else (recur (inc index) state word ended?))

              :single-quote
              (cond
                ;; Standard SQL escapes a quote by doubling it.
                (and (= ch \') (= next-ch \'))
                (recur (+ index 2) state word ended?)
                (= ch \') (recur (inc index) :code word ended?)
                ;; Backslash string escaping varies by database and settings.
                (= ch \\) nil
                :else (recur (inc index) state word ended?))

              :double-quote
              (cond
                (and (= ch \") (= next-ch \"))
                (recur (+ index 2) state word ended?)
                (= ch \") (recur (inc index) :code word ended?)
                :else (recur (inc index) state word ended?))

              :backtick-quote
              (cond
                (and (= ch \`) (= next-ch \`))
                (recur (+ index 2) state word ended?)
                (= ch \`) (recur (inc index) :code word ended?)
                (= ch \\) nil
                :else (recur (inc index) state word ended?))

              :code
              (cond
                (or (= ch \space) (= ch \tab) (= ch \newline)
                    (= ch \return) (= ch \formfeed))
                (recur (inc index) state word ended?)

                (and (= ch \-) (= next-ch \-))
                (recur (+ index 2) :line-comment word ended?)

                (and (= ch \/) (= next-ch \*))
                (recur (+ index 2) :block-comment word ended?)

                (= ch \;)
                (if ended?
                  nil
                  (recur (inc index) state word true))

                ended? nil

                (= ch \') (recur (inc index) :single-quote word ended?)
                (= ch \") (recur (inc index) :double-quote word ended?)
                (= ch \`) (recur (inc index) :backtick-quote word ended?)

                ;; PostgreSQL dollar quoting, bracket quoting, and similar
                ;; extensions need a dialect-specific parser. Fail closed.
                (or (= ch \$) (= ch \[)) nil

                (and (nil? word)
                     (or (<= (int \A) (int ch) (int \Z))
                         (<= (int \a) (int ch) (int \z))))
                (let [end (loop [cursor index]
                            (if (and (< cursor size)
                                     (let [candidate (nth sql cursor)]
                                       (or (<= (int \A) (int candidate) (int \Z))
                                           (<= (int \a) (int candidate) (int \z)))))
                              (recur (inc cursor))
                              cursor))]
                  (recur end state (subs sql index end) ended?))

                ;; A statement whose first token is not a bare word has no
                ;; operation name this generic seam can safely identify.
                (nil? word) nil
                :else (recur (inc index) state word ended?)))))))))

(defn operation-name
  "Return a conservative, low-cardinality SQL operation name, or nil.

  Leading whitespace and ordinary line/block comments are ignored. The result
  is omitted for compound statements, CTEs, dialect-dependent lexical forms,
  truncated input, malformed input, and unknown operations. The returned value
  never contains caller SQL text. This narrow fallback exists because the
  current driver seam does not yet provide operation metadata directly."
  [sql]
  (let [operation (some-> (sql-shape sql) str/upper-case)]
    (when (contains? known-operations operation) operation)))

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
  (or operation system))

(defn- exception-type [error]
  (try
    (or (some-> error class .getName) "UnknownExceptionType")
    (catch :default _ "UnknownExceptionType")))

(defn- record-failure! [span metric-attributes error]
  (try
    (let [error-type (exception-type error)]
      (trace/set-attribute! span :error.type error-type)
      (swap! metric-attributes assoc :error.type error-type)
      (logs/emit! (sdk/logger scope-name {:version instrumentation-version})
                  {:event-name "db.client.operation.exception"
                   :body "database client operation exception"
                   :severity :warn
                   ;; Messages, data, and stack traces can contain query text,
                   ;; values, paths, and credentials. Raw SQLSTATE-like values
                   ;; are likewise untrusted until the driver SPI supplies an
                   ;; explicit bounded error metadata contract.
                   :attributes {:exception.type error-type}})
      (trace/set-status! span :error "database operation failed"))
    (catch :default _ nil)))

(def ^:private result-set-operations #{"SELECT" "EXPLAIN" "CALL"})
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
  (try
    (when (and (capture-row-counts?) (map? result))
      (let [rows (:rows result)
            affected (:count result)]
        (when (and (contains? result-set-operations operation) (vector? rows))
          (trace/set-attribute! span :db.response.returned_rows (count rows)))
        (when (and (contains? mutation-operations operation)
                   (integer? affected) (not (neg? affected)))
          ;; OTel currently standardizes returned rows but not affected rows.
          ;; Keep this useful mutation count in the library-owned namespace.
          (trace/set-attribute! span :jolt.db.response.affected_rows affected))))
    (catch :default _ nil)))

(defn- finish! [span start-wall start-mono metric-attributes]
  ;; Derive both signals from one monotonic interval. The wall-clock anchor is
  ;; used only to place the span on the exported timeline. Finalization is fully
  ;; fail-open so telemetry cannot replace a result or the original Throwable.
  (let [end-mono (try (host/mono-nanos) (catch :default _ nil))
        elapsed (when end-mono (max 0 (- end-mono start-mono)))]
    (try
      (if elapsed
        (trace/end! span (+ start-wall elapsed))
        (trace/end! span))
      (catch :default _ nil))
    (when elapsed
      (try
        (metrics/record! (duration-instrument)
                         (/ elapsed 1000000000.0)
                         @metric-attributes)
        (catch :default _ nil)))))

(defn- traced [db-driver sql proceed]
  (let [descriptor (safe-descriptor db-driver)
        operation (operation-name sql)
        system (system-name descriptor)
        tracer (sdk/tracer scope-name {:version instrumentation-version})
        start-wall (host/wall-nanos)
        start-mono (host/mono-nanos)
        attributes (cond-> {:db.system.name system}
                     operation (assoc :db.operation.name operation))
        metric-attributes (atom attributes)
        span (trace/start-span tracer (span-name operation system)
                               ;; SQL semantic conventions require CLIENT even
                               ;; for embedded SQL engines.
                               {:kind :client
                                :attributes attributes
                                :start-timestamp start-wall})]
    (try
      (trace/with-current-span span
        (try
          (let [result (proceed)]
            (record-result-cardinality! span operation result)
            result)
          (catch :default error
            (record-failure! span metric-attributes error)
            (throw error))))
      (finally
        (finish! span start-wall start-mono metric-attributes)))))

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

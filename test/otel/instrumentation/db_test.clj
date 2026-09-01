(ns otel.instrumentation.db-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [db.driver :as driver]
            [otel.context :as context]
            [otel.exporter.memory :as memory]
            [otel.instrumentation.db :as instrumentation]
            [otel.sdk :as sdk]
            [otel.trace :as trace]))

(defn- test-driver [descriptor]
  (reify driver/Driver
    (descriptor [_] descriptor)
    (open-handle [_ _] nil)
    (close-handle [_ _] nil)
    (execute-handle [_ _ _ _] {:labels [] :rows [] :count 0})))

(defn- join-point []
  {:id :db.jdbc-shim/execute
   :advice-role :db/client
   :contract :args-v1
   :match {:ns 'db.jdbc-shim :call 'db.driver/execute-handle :arity 4}
   :library {:id 'jolt-lang/db :version instrumentation/db-build-id}})

(defn- with-memory-sdk [f]
  (let [exporter (memory/exporter)
        handle (sdk/init! {:service-name "db-instrumentation-test"
                           :exporter exporter
                           :processor :simple
                           :metrics? false})]
    (try
      (f exporter)
      (finally
        (sdk/shutdown! handle)))))

(deftest operation-classification-is-bounded-and-closed
  (doseq [[sql expected]
          [[" SELECT * FROM private_customer" "SELECT"]
           ["-- private tenant\ninsert into t values (?)" "INSERT"]
           ["/* secret */ UPDATE t SET token = ?" "UPDATE"]
           ["vacuum private_customer" "UNKNOWN"]
           ["totally-secret-unparseable" "UNKNOWN"]
           [nil "UNKNOWN"]]]
    (is (= expected (instrumentation/operation-name sql)))))

(deftest result-identity-parentage-and-safe-attributes
  (with-memory-sdk
    (fn [exporter]
      (let [db-driver (test-driver {:id :chdb :product-name "chDB"})
            result {:labels ["private"] :rows [["secret-row"]] :count 1}
            tracer (sdk/tracer "db-instrumentation-parent")
            observed
            (trace/with-span [_parent tracer "request"]
              (instrumentation/around
               (join-point)
               [db-driver :secret-handle
                "SELECT private_value FROM customer WHERE token = ?"
                ["secret-parameter"]]
               (fn [] result)))
            spans (memory/spans exporter)
            parent (first (filter #(= "request" (:name %)) spans))
            database (first (filter #(= "SELECT" (:name %)) spans))
            serialized (pr-str database)]
        (is (identical? result observed))
        (is (= :internal (:kind database)))
        (is (= (get-in parent [:span-context :span-id])
               (:parent-span-id database)))
        (is (= "clickhouse" (get (:attributes database) "db.system.name")))
        (is (= "SELECT" (get (:attributes database) "db.operation.name")))
        (is (= 1 (get (:attributes database) "db.response.returned_rows")))
        (is (nil? (get (:attributes database) "jolt.db.response.affected_rows")))
        (doseq [secret ["private_value" "customer" "secret-parameter"
                        "secret-handle" "secret-row"]]
          (is (not (.contains serialized secret))))))))

(deftest remote-driver-uses-client-kind
  (with-memory-sdk
    (fn [exporter]
      (instrumentation/around
       (join-point)
       [(test-driver {:id :postgresql :product-name "PostgreSQL"})
        nil "DELETE FROM t" []]
       (fn [] {:labels [] :rows [] :count 2}))
      (let [span (first (memory/spans exporter))]
        (is (= "DELETE" (:name span)))
        (is (= :client (:kind span)))
        (is (= 2 (get (:attributes span) "jolt.db.response.affected_rows")))
        (is (nil? (get (:attributes span) "db.response.returned_rows")))))))

(deftest malformed-result-cardinality-is-ignored
  (with-memory-sdk
    (fn [exporter]
      (let [result {:rows (Object.) :count -1}
            observed (instrumentation/around
                      (join-point)
                      [(test-driver {:id :duckdb}) nil "SELECT 1" []]
                      (fn [] result))
            span (first (memory/spans exporter))]
        (is (identical? result observed))
        (is (nil? (get (:attributes span) "db.response.returned_rows")))
        (is (nil? (get (:attributes span) "jolt.db.response.affected_rows")))))))

(deftest exception-identity-and-message-privacy
  (with-memory-sdk
    (fn [exporter]
      (let [failure (ex-info "password super-secret" {:token "also-secret"})
            observed (try
                       (instrumentation/around
                        (join-point)
                        [(test-driver {:id :duckdb :product-name "DuckDB"})
                         nil "CALL private_procedure(?)" ["private-arg"]]
                        (fn [] (throw failure)))
                       (catch :default error error))
            span (first (memory/spans exporter))
            serialized (pr-str span)]
        (is (identical? failure observed))
        (is (= :error (get-in span [:status :code])))
        (is (= "database operation failed"
               (get-in span [:status :description])))
        (is (= "CALL" (get (:attributes span) "db.operation.name")))
        (is (= "clojure.lang.ExceptionInfo"
               (get (:attributes span) "error.type")))
        (doseq [secret ["super-secret" "also-secret" "private_procedure"
                        "private-arg"]]
          (is (not (.contains serialized secret))))))))

(deftest malformed-descriptor-does-not-change-application-result
  (with-memory-sdk
    (fn [exporter]
      (let [db-driver
            (reify driver/Driver
              (descriptor [_] (throw (ex-info "descriptor secret" {})))
              (open-handle [_ _] nil)
              (close-handle [_ _] nil)
              (execute-handle [_ _ _ _] nil))
            result (Object.)
            observed (instrumentation/around
                      (join-point) [db-driver nil "nonsense" []]
                      (fn [] result))
            span (first (memory/spans exporter))]
        (is (identical? result observed))
        (is (= "other_sql" (:name span)))
        (is (not (.contains (pr-str span) "descriptor secret")))))))

(deftest suppression-bypasses-all-instrumentation-side-observation
  (with-memory-sdk
    (fn [exporter]
      (let [descriptor-called? (atom false)
            db-driver
            (reify driver/Driver
              (descriptor [_]
                (reset! descriptor-called? true)
                (throw (ex-info "must not inspect" {})))
              (open-handle [_ _] nil)
              (close-handle [_ _] nil)
              (execute-handle [_ _ _ _] nil))
            result (Object.)
            observed
            (context/with-instrumentation-suppressed
              (instrumentation/around
               (join-point)
               [db-driver :private-handle "SELECT private" ["private"]]
               (fn [] result)))]
        (is (identical? result observed))
        (is (false? @descriptor-called?))
        (is (empty? (memory/spans exporter)))))))

(deftest provider-contract-is-exact
  (is (= {:schema 1
          :libraries {'jolt-lang/db instrumentation/db-build-id}
          :roles {:db/client
                  {:fn 'otel.instrumentation.db/around
                   :contract :args-v1}}}
         instrumentation/aspect-provider)))

(deftest provider-version-matches-the-fetched-library-manifest
  (let [resource (io/resource "META-INF/jolt/aspects/db-jdbc-shim.edn")
        manifest (some-> resource slurp edn/read-string)]
    (is (some? resource))
    (is (= {:schema 1
            :library {:id 'jolt-lang/db
                      :version instrumentation/db-build-id}
            :aspects
            [{:id :db.jdbc-shim/execute
              :match {:ns 'db.jdbc-shim
                      :call 'db.driver/execute-handle
                      :arity 4}
              :advice-role :db/client
              :expect {:matches 1}}]}
           manifest))))

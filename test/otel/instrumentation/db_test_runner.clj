(ns otel.instrumentation.db-test-runner
  (:require [clojure.test :as test]
            [otel.instrumentation.db-test]))

(defn -main [& _]
  (let [result (test/run-tests 'otel.instrumentation.db-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (System/exit 1))))

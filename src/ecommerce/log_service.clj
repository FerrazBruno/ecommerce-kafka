(ns ecommerce.log-service
  (:require [ecommerce.core-kafka-service :as s])
  (:import  (java.util.regex Pattern)))


(defn parse-log
  [record]
  (println "\n=========================================="
           "\nLOG"
           "\nKey:"       (.key record)
           "\nValue:"     (.value record)
           "\nTopic:"     (.topic record)
           "\nPartition:" (.partition record)
           "\nOffset:"    (.offset record)
           "\n==========================================\n"))


(defn -main
  []
  (s/kafka-service "log-service" (Pattern/compile "ECOMMERCE.*") parse-log))

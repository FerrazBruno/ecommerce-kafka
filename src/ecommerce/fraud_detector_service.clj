(ns ecommerce.fraud-detector-service
  (:require [ecommerce.core-kafka-service :as s]))


(defn parse-fraud
  [record]
  (println "\n=========================================="
           "\nMessage received"
           "\nKey:"       (.key record)
           "\nValue:"     (.value record)
           "\nTopic:"     (.topic record)
           "\nPartition:" (.partition record)
           "\nOffset:"    (.offset record)
           "\n==========================================\n"))


(defn -main
  []
  (s/kafka-service "fraud-detector-service" "ECOMMERCE_NEW_ORDER" parse-fraud))

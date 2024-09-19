(ns ecommerce.email-service
  (:require [ecommerce.core-kafka-service :as s]))


(defn parse-email
  [record]
  (println "\n=========================================="
           "\nSend Email"
           "\nKey:"       (.key record)
           "\nValue:"     (.value record)
           "\nTopic:"     (.topic record)
           "\nPartition:" (.partition record)
           "\nOffset:"    (.offset record)
           "\n==========================================\n"))


(defn -main
  []
  (s/kafka-service "email-service" "ECOMMERCE_SEND_EMAIL" parse-email))

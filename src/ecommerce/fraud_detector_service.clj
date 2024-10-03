(ns ecommerce.fraud-detector-service
  (:require [ecommerce.core-kafka-service :as s]
            [ecommerce.core-kafka-producer :as p]
            [cheshire.core :as che]))


(defn is-fraud?
  [order]
  ;; pretending that the fraud happens when the amount is >= 4500
  (>= (:amount order) 4500))


(defn message-view
  [record]
  (println "\n=========================================="
           "\nMessage received"
           "\nKey:"       (.key record)
           "\nValue:"     (.value record)
           "\nTopic:"     (.topic record)
           "\nPartition:" (.partition record)
           "\nOffset:"    (.offset record)
           "\n==========================================\n"))


(defn parse-fraud
  [record]
  (message-view record)
  (let [order (che/parse-string (.value record) true)
        user-id (:user-id order)]
    (if (is-fraud? order)
      (try
        (doall
         (p/send-message "ECOMMERCE_ORDER_REJECTED" user-id order)
         (println "Order is a fraud!!!"))
        (catch Exception e
          (println (str "Caught Exception: " (.getMessage e)))))
      (try
        (doall
         (p/send-message "ECOMMERCE_ORDER_APPROVED" user-id order)
         (println "Approved: " order))
        (catch Exception e
          (println (str "Caught Exception: " (.getMessage e))))))))


(defn -main
  []
  (s/kafka-service "fraud-detector-service" "ECOMMERCE_NEW_ORDER" parse-fraud))

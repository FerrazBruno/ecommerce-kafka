(ns ecommerce.new-order
  (:require [ecommerce.core-kafka-producer :as p]))


(def email
  "Thank you for your order! We are processing your order!")


(defn key-uuid
  []
  (str (java.util.UUID/randomUUID)))


(defn -main
  []
  (doseq [_ (range 10)]
    (p/send-message "ECOMMERCE_NEW_ORDER" (key-uuid) "1234,4321,330")
    (p/send-message "ECOMMERCE_SEND_EMAIL" (key-uuid) email)))

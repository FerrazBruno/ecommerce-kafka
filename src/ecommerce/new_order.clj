(ns ecommerce.new-order
  (:require [ecommerce.core-kafka-producer :as p]))


(def email
  "Thank you for your order! We are processing your order!")


(defn uuid
  []
  (str (java.util.UUID/randomUUID)))


(defn order
  [user-id]
  {:user-id user-id
   :order-id (uuid)
   :amount (+ (rand-int 5000) 1)})


(defn -main
  []
  (let [user-id (uuid)]
    (p/send-message "ECOMMERCE_NEW_ORDER" user-id (order user-id))
    (p/send-message "ECOMMERCE_SEND_EMAIL" user-id email)))

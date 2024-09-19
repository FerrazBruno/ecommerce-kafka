(ns ecommerce.log-service
  (:import
   (org.apache.kafka.clients.consumer KafkaConsumer)
   (java.util Properties)
   (java.util.regex Pattern)))


(defn create-consumer
  []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers"  "localhost:9092")
                (.put "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "group.id"           "log-service"))]
    (KafkaConsumer. props )))


(defn consume-messages
  [consumer]
  ;; Se inscrevendo no topico
  (.subscribe consumer (Pattern/compile "ECOMMERCE.*"))
  (println "Consumer subscribed to the topic!")
  ;; Loop infinito para consumir mensagens
  (while true
    (let [records (.poll consumer 1000)] ;; Polling com timeout de 1000ms
      (doseq [record records]
        (println "\n=========================================="
                 "\nLOG"
                 "\nKey:"       (.key record)
                 "\nValue:"     (.value record)
                 "\nTopic:"     (.topic record)
                 "\nPartition:" (.partition record)
                 "\nOffset:"    (.offset record)
                 "\n==========================================\n")))))


(defn -main
  []
  (let [consumer (create-consumer)]
    (consume-messages consumer)))

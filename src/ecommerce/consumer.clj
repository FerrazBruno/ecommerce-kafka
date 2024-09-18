(ns ecommerce.consumer
  (:import
   (org.apache.kafka.clients.consumer KafkaConsumer)
   (java.util Properties Collections)))


(defn create-consumer
  []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers"  "localhost:9092")
                (.put "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "group.id"           "consumidor"))]
    (KafkaConsumer. props )))


(defn consume-messages
  [consumer topic]
  ;; Se inscrevendo no topico
  (.subscribe consumer (Collections/singletonList topic))
  (println "Consumidor inscrito no topico")
  ;; Loop infinito para consumir mensagens
  (while true
    (let [records (.poll consumer 1000)] ;; Polling com timeout de 1000ms
      (doseq [record records]
        (println "\n=========================================="
                 "\nMessage received"
                 "\nKey:"       (.key record)
                 "\nValue:"     (.value record)
                 "\nTopic:"     (.topic record)
                 "\nPartition:" (.partition record)
                 "\nOffset:"    (.offset record)
                 "\n==========================================")))))


(defn -main
  []
  (let [consumer (create-consumer)]
    (consume-messages consumer "ECOMMERCE_NEW_ORDER")))

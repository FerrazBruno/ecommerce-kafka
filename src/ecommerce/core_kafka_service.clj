(ns ecommerce.core-kafka-service
  (:import
   (org.apache.kafka.clients.consumer KafkaConsumer)
   (java.util Properties Collections))
  (:gen-class))


(defn create-consumer
  [group-id]
  (let [props (doto (Properties.)
                (.put "bootstrap.servers"  "localhost:9092")
                (.put "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
                (.put "group.id"           group-id)
                (.put "max.poll.records"   "1")
                (.put "client.id"          (str (java.util.UUID/randomUUID))))]
    (KafkaConsumer. props)))


(defn run
  [consumer fn-parse]
  (while true
    (let [records (.poll consumer 100)]
      (when (not (.isEmpty records))
        (println (str "Found " (.count records) " message(s)."))
        (doseq [record records]
          (fn-parse record))))))


(defn kafka-service
  [group-id topic fn-parse]
  (let [consumer (create-consumer group-id)]
    (.subscribe consumer (Collections/singletonList topic))
    (println "Consumer subscribed to the topic!")
    (run consumer fn-parse)))


(defn -main
  []
  (println "Hello World!"))

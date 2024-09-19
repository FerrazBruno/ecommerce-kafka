(ns ecommerce.core-kafka-producer
  (:import
   (org.apache.kafka.clients.producer KafkaProducer ProducerRecord Callback)
   (java.util Properties)))


(defn create-producer
  []
  (let [props (doto (Properties.)
                (.put "bootstrap.servers" "localhost:9092")
                (.put "key.serializer"    "org.apache.kafka.common.serialization.StringSerializer")
                (.put "value.serializer"  "org.apache.kafka.common.serialization.StringSerializer"))]
    (KafkaProducer. props)))


(defn callback
  []
  (reify Callback
    (onCompletion [_this metadata exception]
      (if exception
        (println "Error sending message:" (.getMessage exception))
        (println "\nMensagem sent successfully!"
                 "\nTopic:"     (.topic metadata)
                 "\nPartition:" (.partition metadata)
                 "\nOffset:"    (.offset metadata)
                 "\n")))))


(defn send-message
  "Envia uma mensagem para o tópico Kafka fornecido com um callback para tratar a resposta."
  [topic key value]
  (let [producer (create-producer)
        record   (ProducerRecord. topic key value)]
    (.send producer record (callback))
    (.close producer)))

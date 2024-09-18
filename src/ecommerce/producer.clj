(ns ecommerce.producer
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


(defn send-message
   "Envia uma mensagem para o tópico Kafka fornecido com um callback para tratar a resposta."
   [producer topic key value]
   (let [record (ProducerRecord. topic key value)]
     (.send producer record
            ;; Callback para tratar a resposta de sucesso ou falha
            (reify Callback
              (onCompletion [_this metadata exception]
                (if exception
                  (println "Error sending message:" (.getMessage exception))
                  (println "Mensagem sent successfully!"
                           "Topic:" (.topic metadata)
                           "Partition:" (.partition metadata)
                           "Offset:" (.offset metadata))))))))


(defn -main
  "Ponto de entrada da aplicação. Envia uma mensagem simples ao Kafka."
  []
  (let [producer (create-producer)]
    (send-message producer "ECOMMERCE_NEW_ORDER" "1234,4321,330" "1234,4321,330")
    (println "Message sent!")
    (.close producer)))

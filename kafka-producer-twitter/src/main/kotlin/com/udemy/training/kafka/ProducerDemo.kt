package com.udemy.training.kafka

import com.udemy.training.kafka.Constants.Companion.BOOTSTRAP_SERVER
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.Properties

class ProducerDemo {

    companion object {
        fun getProducerProperties(keySerializer: String, valueSerializer: String): Properties {
            val properties = Properties()
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)

            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString())
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString())
            return properties
        }
    }

    fun producerTest() {
        val logger = LoggerFactory.getLogger(this::class.java)
        val properties = getProducerProperties(StringSerializer::class.java.name, StringSerializer::class.java.name)
        val producerRecord = getProducerRecord("first_topic", "Viva Saprissa!!!")
        val kafkaProducer = KafkaProducer<String, String>(properties)

        kafkaProducer.send(producerRecord) { recordMetadata: RecordMetadata, exception: Exception? ->
            if (exception == null) {
                logger.info("Producer metadata received: " + "\n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp())
            } else {
                logger.error("Error producing data ", exception)
            }
        }

        kafkaProducer.flush()
        kafkaProducer.close()
    }

    private fun <V> getProducerRecord(topic: String, value: V): ProducerRecord<String, V> {
        return ProducerRecord(topic, value)
    }

}

fun main(args: Array<String>) {
    val p = ProducerDemo()
    p.producerTest()
}
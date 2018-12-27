package com.udemy.traning.kafka

import com.udemy.traning.kafka.Constants.Companion.BOOTSTRAP_SERVER
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.Properties

class ProducerDemo {

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

    private fun getProducerProperties(keySerializer: String, valueSerializer: String): Properties {
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
        return properties
    }

}

fun main(args: Array<String>) {
    val p = ProducerDemo()
    p.producerTest()
}
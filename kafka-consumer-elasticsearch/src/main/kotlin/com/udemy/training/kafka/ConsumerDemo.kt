package com.udemy.training.kafka

import com.udemy.training.kafka.Constants.Companion.BOOTSTRAP_SERVER
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class ConsumerDemo {

    companion object {
        fun getConsumerProperties(keyDeserializer: String, valueDeserializer: String, groupId: String,
                                  offsetReset: String): Properties {
            val properties = Properties()
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset)
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            return properties
        }
    }

    fun consumerTest() {
        val logger = LoggerFactory.getLogger(this::class.java)
        val properties = getConsumerProperties(StringDeserializer::class.java.name, StringDeserializer::class.java.name,
                "consumer-app", "earliest")

        val kafkaConsumer = KafkaConsumer<String, String>(properties)
        kafkaConsumer.subscribe(Arrays.asList("first_topic"))

        while (true) {
            val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100))
            consumerRecords.forEach {
                logger.info("Key: " + it.key() + " Value: " + it.value() + "\n" +
                        "Partition: " + it.partition() + "\n" +
                        "Offset: " + it.offset() + "\n")
            }
        }
    }

}

fun main(args: Array<String>) {
    val consumerDemo = ConsumerDemo()
    consumerDemo.consumerTest()
}
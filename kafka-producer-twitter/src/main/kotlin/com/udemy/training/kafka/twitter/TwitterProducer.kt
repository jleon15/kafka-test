package com.udemy.training.kafka.twitter

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
import com.udemy.training.kafka.ProducerDemo
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit


class TwitterProducer {

    private val logger = LoggerFactory.getLogger(TwitterProducer::class.java)

    fun runTwitterProducer() {
        val messageQueue = LinkedBlockingQueue<String>(1000000)
        val twitterClient = createTwitterClient(messageQueue)
        twitterClient.connect()

        val properties = ProducerDemo.getProducerProperties(StringSerializer::class.java.name, StringSerializer::class.java.name)
        val kafkaProducer = KafkaProducer<String, String>(properties)

        while (!twitterClient.isDone) {
            val message = messageQueue.poll(5, TimeUnit.SECONDS)
            if (message != null) {
                kafkaProducer.send(ProducerRecord("twitter_tweets", null, message),
                        Callback { recordMetadata: RecordMetadata, exception: Exception? ->
                            if (exception != null) {
                                logger.error("An error occurred while producing", exception)
                            }
                        })
            }
        }

        twitterClient.stop()
        kafkaProducer.close()
    }

    private fun createTwitterClient(messageQueue: BlockingQueue<String>): Client {
        val hosebirdHosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()

        val terms = listOf("kafka", "tennis", "soccer")
        hosebirdEndpoint.trackTerms(terms)
        val propertiesFile = FileInputStream(ClassLoader.getSystemResource("twitter.authentication.properties").file)
        val properties = Properties()
        properties.load(propertiesFile)

        val hosebirdAuth = OAuth1(properties.getProperty("consumerKey"), properties.getProperty("consumerSecret"),
                properties.getProperty("token"), properties.getProperty("tokenSecret"))

        val builder = ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(StringDelimitedProcessor(messageQueue))

        return builder.build()
    }


}

fun main(args: Array<String>) {
    val twitterProducer = TwitterProducer()
    twitterProducer.runTwitterProducer()
}
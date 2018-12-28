package com.udemy.training.kafka

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.time.Duration
import java.util.*

class ElasticSearchConsumer {

    fun createClient(): RestHighLevelClient {
        val propertiesFile = FileInputStream(ClassLoader.getSystemResource("elasticsearch.credentials.properties").file)
        val properties = Properties()
        properties.load(propertiesFile)

        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY,
                UsernamePasswordCredentials(properties.getProperty("username"), properties.getProperty("password")))

        val restClientBuilder = RestClient.builder(
                HttpHost(properties.getProperty("hostname"), 443, "https"))
                .setHttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                }

        return RestHighLevelClient(restClientBuilder)

    }


}

fun main(args: Array<String>) {
    val logger = LoggerFactory.getLogger(ElasticSearchConsumer::class.java)
    val elasticSearchClient = ElasticSearchConsumer().createClient()


    val properties = ConsumerDemo.getConsumerProperties(StringDeserializer::class.java.name, StringDeserializer::class.java.name,
            "elastic-search", "earliest")

    val kafkaConsumer = KafkaConsumer<String, String>(properties)
    kafkaConsumer.subscribe(Arrays.asList("twitter_tweets"))

    while (true) {
        val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100))
        val bulkRequest = BulkRequest()
        consumerRecords.forEach {
            val id = it.topic() + "_" + it.partition() + "_" + it.offset()
            val indexRequest = IndexRequest("twitter", "tweets")
                    .source(it.value(), XContentType.JSON, id)

            bulkRequest.add(indexRequest)
        }
        if (consumerRecords.count() > 0) {
            elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
            kafkaConsumer.commitSync()
        }
    }

}
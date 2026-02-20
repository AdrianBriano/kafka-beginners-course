package io.conduktor.demos.kafka.opensearch;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.CredentialsStore;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    final static String OPENSEARCH_INDEX = "wikimedia";

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost("http", connUri.getHost(), connUri.getPort())));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            AuthScope authScope = new AuthScope(null, null, -1, null, null);
            CredentialsStore cp = new BasicCredentialsProvider();
            cp.setCredentials(authScope, new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost("http", connUri.getHost(), connUri.getPort()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        RestHighLevelClient restHighLevelClient = createOpenSearchClient();

        KafkaConsumer<String,String> consumer = createKafkaConsumer();

        try (restHighLevelClient; consumer) {
            if (restHighLevelClient.indices().exists(new GetIndexRequest(OPENSEARCH_INDEX), RequestOptions.DEFAULT)) {
                logger.info("The Wikimedia index already exists!");
            } else {
                CreateIndexRequest createIndexRequest =  new CreateIndexRequest(OPENSEARCH_INDEX);
                createOpenSearchClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The Wikimedia index has been created!");
            }

            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                logger.info("Received " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record: records) {
                    IndexRequest indexRequest = new IndexRequest(OPENSEARCH_INDEX).source(record.value(), XContentType.JSON);

                    IndexResponse indexResponse = createOpenSearchClient().index(indexRequest, RequestOptions.DEFAULT);

                    logger.info("Inserted document with id " + indexResponse.getId() +" in the index " + OPENSEARCH_INDEX);
                }
            }
        } catch (IOException e) {
            logger.error("Error while calling opensearch client.", e);
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        Properties properties =  new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        return new KafkaConsumer<>(properties);
    }
}

package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) {
        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String TOPIC = "wikimedia.recentchange";
        final String WIKIMEDIA_RECENT_CHANGE_URL = "http://stream.wikimedia.org/v2/stream/recentchange";
        final URI wikimediaURI = URI.create(WIKIMEDIA_RECENT_CHANGE_URL);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC);

        EventSource.Builder eventSourceBuilder = new EventSource.Builder(ConnectStrategy.http(wikimediaURI).header("User-Agent","WikimediaKafkaConnector/1.0 (https://github.com/conduktor/kafka-connect-wikimedia)"));

        try (BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder).build()) {

            backgroundEventSource.start();

            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            logger.error("Error while producing data", e);
        }
    }
}

package com.github.dhoard.orders.kafka.streams;

import com.github.dhoard.orders.kafka.streams.serde.JsonObjectSerde;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final Serde<String> keySerdes = Serdes.String();

    private static final Serde<JsonObject> valueSerdes = new JsonObjectSerde();

    public static void main(String[] args) {
        new Main().run();
    }

    public void run() {
        LOGGER.info("Application starting");

        String bootstrapServers = "cp-7-2-x:9092";
        String applicationId = "orders";
        Class<?> keySerde = keySerdes.getClass();
        Class<?> valueSerde = valueSerdes.getClass();
        String autoOffsetResetConfig = "earliest";
        int numStreamThreads = Runtime.getRuntime().availableProcessors() * 2;
        long cacheMaxBytesBuffering = 0;
        String stateDir = "/tmp/state";

        LOGGER.info("bootstrapServers       = [" + bootstrapServers + "]");
        LOGGER.info("autoOffsetResetConfig  = [" + autoOffsetResetConfig + "]");
        LOGGER.info("applicationId          = [" + applicationId + "]");
        LOGGER.info("defaultKeySerde        = [" + keySerde + "]");
        LOGGER.info("defaultValueSerde      = [" + valueSerde + "]");
        LOGGER.info("numStreamThreads       = [" + numStreamThreads + "]");
        LOGGER.info("cacheMaxBytesBuffering = [" + cacheMaxBytesBuffering + "]");
        LOGGER.info("stateDir               = [" + stateDir + "]");

        Properties properties = new Properties();
        
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numStreamThreads);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, cacheMaxBytesBuffering);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        properties.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        Topology topology = buildTopology();
        LOGGER.info(topology.describe().toString());
        
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        kafkaStreams.setGlobalStateRestoreListener(
            new StateRestoreListener() {
                @Override
                public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffest) {
                    LOGGER.info(
                            String.format(
                                    "onRestoreStart() topicPartition [%s] storeName [%s] offset [%d]",
                                    topicPartition.partition(),
                                    storeName,
                                    startingOffset,
                                    endingOffest));
                }

                @Override
                public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                    // DO NOTHING
                }

                @Override
                public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                    LOGGER.info(
                            String.format(
                                    "onRestoreEnd() topicPartition [%s] storeName [%s] totalRestored [%d]",
                                    topicPartition.partition(),
                                    storeName,
                                    totalRestored));
                }
            }
        );

        kafkaStreams.start();
    }

    private Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // convert order events without a key to order events with a key
        streamsBuilder
                .stream("order", Consumed.with(keySerdes, valueSerdes))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .map((key, jsonObject) -> {
                    key = jsonObject.get("order.id").getAsString();
                    JsonObject result = jsonObject.deepCopy();
                    return new KeyValue<>(key, result);
                })
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .to("order-keyed");

        // aggregate order events by order.id (order.placed + order.completed) and generate an order.info event
        streamsBuilder
                .stream("order-keyed", Consumed.with(keySerdes, valueSerdes))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .groupByKey()
                .aggregate(
                        JsonObject::new,
                        (key, jsonObject, aggregateJsonObject) -> {
                            // if the aggregated order object is empty, use the order object
                            if (aggregateJsonObject.size() == 0) {
                                JsonElement jsonElement = jsonObject.get("event.type");
                                if (jsonElement == null) {
                                    // bad message, ignore
                                    return null;
                                }

                                if ("order.placed".equals(jsonElement.getAsString())) {
                                    // order.placed event
                                    return jsonObject;
                                }

                                // unknown message, ignore
                                return null;
                            }

                            JsonElement jsonElement = jsonObject.get("event.type");

                            if (jsonElement == null) {
                                // bad message, ignore
                                return null;
                            }

                            if ("order.fulfilled".equals(jsonElement.getAsString())) {
                                // use "event.timestamp" from the event
                                long orderPlacedTimestamp = aggregateJsonObject.get("event.timestamp").getAsLong();
                                long orderCompletedTimestamp = jsonObject.get("event.timestamp").getAsLong();
                                long processingMs = orderCompletedTimestamp - orderPlacedTimestamp;

                                JsonObject result = new JsonObject();
                                result.addProperty("event.type", "order.info");
                                result.addProperty("facility.id", jsonObject.get("facility.id").getAsString());
                                result.addProperty("order.id", jsonObject.get("order.id").getAsString());
                                result.addProperty("processing.ms", processingMs);
                                return result;
                            }

                            // unknown message, ignore
                            return null;
                        }, Materialized.as("order-state-store"))
                .filterNot((k, v) -> v == null)
                .filter((key, jsonObject) -> {
                    // completion criteria, existence of "processing.ms"
                    return jsonObject.has("processing.ms");
                })
                .toStream()
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .to("order-info");

        // re-key order.info events to use facility.info event
        streamsBuilder
                .stream("order-info", Consumed.with(keySerdes, valueSerdes))
                .filter((s, jsonObject) -> jsonObject != null)
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .map((key, jsonObject) -> {
                    // re-key based on facility id
                    key = jsonObject.get("facility.id").getAsString();
                    JsonObject result = jsonObject.deepCopy();
                    result.addProperty("event.type", "facility.info");
                    return new KeyValue<>(key, result);
                })
                .peek((k, v) -> LOGGER.info(String.format("facility.info [%s] = [%s]", k, v)))
                .to("facility-info");

        // aggregate facility.info events by facility.id with a 1-minute tumbling window
        streamsBuilder
                .stream("facility-info", Consumed.with(keySerdes, valueSerdes))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(0)))
                .aggregate(
                        JsonObject::new,
                        (key, jsonObject, aggregateJsonObject) -> {
                            // if the aggregated order info object is empty, use the order info object
                            if (aggregateJsonObject.size() == 0) {
                                JsonElement jsonElement = jsonObject.get("event.type");
                                if (jsonElement == null) {
                                    // bad message, ignore
                                    return null;
                                }

                                if ("facility.info".equals(jsonElement.getAsString())) {
                                    String facilityId = jsonObject.get("facility.id").getAsString();
                                    long processingCount = 1;
                                    long processingMs = jsonObject.get("processing.ms").getAsLong();

                                    JsonObject result = new JsonObject();
                                    result.addProperty("event.type", "facility.info");
                                    result.addProperty("facility.id", facilityId);
                                    result.addProperty("processing.count", processingCount);
                                    result.addProperty("processing.ms", processingMs);
                                    return result;
                                }

                                // unknown message, ignore
                                return null;
                            }

                            String facilityId = jsonObject.get("facility.id").getAsString();

                            long processingCount = aggregateJsonObject.get("processing.count").getAsLong() + 1;

                            long processingMs =
                                    jsonObject.get("processing.ms").getAsLong()
                                        + aggregateJsonObject.get("processing.ms").getAsLong();

                            JsonObject result = new JsonObject();
                            result.addProperty("event.type", "facility.info");
                            result.addProperty("facility.id", facilityId);
                            result.addProperty("processing.count", processingCount);
                            result.addProperty("processing.ms", processingMs);

                            return result;
                        }, Materialized.as("facility-info-state-store"))
                .filter((string, jsonObject) -> jsonObject != null)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .map((w, v) -> KeyValue.pair(w.key(), v))
                .peek((k, v) -> LOGGER.info(String.format("facility.info [%s] = [%s]", k, v)))
                .to("facility-info-by-minute", Produced.with(keySerdes, valueSerdes).as("facility-info-by-minute"));

        return streamsBuilder.build();
    }
}

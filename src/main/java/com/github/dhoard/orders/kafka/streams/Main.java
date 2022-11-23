/*
 * Copyright 2022 Douglas Hoard
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dhoard.orders.kafka.streams;

import com.github.dhoard.kafka.serde.gson.JsonObjectSerde;
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

    private static final Serde<String> stringSerde = Serdes.String();

    private static final Serde<JsonObject> jsonObjectSerde = new JsonObjectSerde();

    public static void main(String[] args) {
        new Main().run();
    }

    public void run() {
        LOGGER.info("Application starting");

        String bootstrapServers = "cp-7-2-x:9092";
        String applicationId = "orders";
        Class<?> keySerde = stringSerde.getClass();
        Class<?> valueSerde = jsonObjectSerde.getClass();
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
                public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
                    LOGGER.info(
                            String.format(
                                    "onRestoreStart() topicPartition [%s] storeName [%s] startingOffset [%d] endingOffset [%d]",
                                    topicPartition.partition(),
                                    storeName,
                                    startingOffset,
                                    endingOffset));
                }

                @Override
                public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
                    // DO NOTHING
                }

                @Override
                public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
                    if (totalRestored > 0) {
                        LOGGER.info(
                                String.format(
                                        "onRestoreEnd() topicPartition [%s] storeName [%s] totalRestored [%d]",
                                        topicPartition.partition(),
                                        storeName,
                                        totalRestored));
                    }
                }
            }
        );

        kafkaStreams.start();
    }

    private Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // re-key order events (("order.placed" and "order.fulfilled") without a key to
        // order events keyed by "order.id"
        streamsBuilder
                .stream("order", Consumed.with(stringSerde, jsonObjectSerde))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .map((k, v) -> new KeyValue<>(v.get("order.id").getAsString(), v))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .to("order-keyed");

        // aggregate order events ("order.placed" + "order.fulfilled") and
        // output an "order.info" event, keyed by "order.id"
        streamsBuilder
                .stream("order-keyed", Consumed.with(stringSerde, jsonObjectSerde))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .process(OrderProcessor.supplier())
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .to("order-info");

        // convert "order.info" events to into "facility.info" events, keyed by "facility.id"
        streamsBuilder
                .stream("order-info", Consumed.with(stringSerde, jsonObjectSerde))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .map((k, v) -> {
                    // re-key based on facility id
                    v.addProperty("event.type", "facility.info");
                    return new KeyValue<>(v.get("facility.id").getAsString(), v);
                })
                .peek((k, v) -> LOGGER.info(String.format("facility.info [%s] = [%s]", k, v)))
                .to("facility-info");

        // aggregate "facility.info" events by "facility.id" with a 1-minute tumbling window
        streamsBuilder
                .stream("facility-info", Consumed.with(stringSerde, jsonObjectSerde))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(60)))
                .aggregate(
                        JsonObject::new,
                        (k, v, a) -> {
                            JsonObject result = new JsonObject();
                            result.addProperty("event.type", "facility.info");
                            result.addProperty("facility.id", k);

                            long processingCount = 1;
                            long processingMs = v.get("processing.ms").getAsLong();

                            if (a.size() > 0) {
                                // previous aggregate
                                long aggregateProcessingCount = a.get("processing.count").getAsLong();
                                long aggregateProcessingMs = a.get("processing.ms").getAsLong();

                                aggregateProcessingCount += processingCount;
                                aggregateProcessingMs += processingMs;

                                result.addProperty("processing.count", aggregateProcessingCount);
                                result.addProperty("processing.ms", aggregateProcessingMs);
                            } else {
                                // set up the initial aggregate
                                result.addProperty("processing.count", processingCount);
                                result.addProperty("processing.ms", processingMs);
                            }
                            return result;
                        }, Materialized.as("facility-info-state-store"))
                .filter((k, v) -> v != null)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .map((w, v) -> {
                    v.addProperty("event.timestamp", w.window().endTime().toEpochMilli());
                    return KeyValue.pair(w.key(), v);
                })
                .peek((k, v) -> LOGGER.info(String.format("facility.info [%s] = [%s]", k, v)))
                .to("facility-info-by-minute");

        return streamsBuilder.build();
    }
}

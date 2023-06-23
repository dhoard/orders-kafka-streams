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

import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    // Configuration
    private static final String BOOTSTRAP_SERVERS = "cp-7-4-x:9092";
    private static final String APPLICATION_ID = "orders";
    private static final int NUM_STREAM_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int CACHE_MAX_BYTES = 0;
    private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";
    private static final String STATE_DIR = "/tmp/state";
    private static final String SCHEMA_REGISTRY_URL = "http://cp-7-4-x:8081";
    private static final String TOPIC = "order";
    public static final String CACHE_MAX_BYTES_CONFIG = "cache.max.bytes";

    private Serdes.StringSerde keySerde = new Serdes.StringSerde();
    private KafkaJsonSchemaSerde<OrderEvent> orderEventSerde;
    private KafkaJsonSchemaSerde<OrderInfoEvent> orderInfoEventSerde;
    private KafkaJsonSchemaSerde<FacilityInfoEvent> facilityInfoEventSerde;

    public static void main(String[] args) {
        new Main().run();
    }

    public void run() {
        LOGGER.info("Application starting");

        LOGGER.info("bootstrapServers       = [" + BOOTSTRAP_SERVERS + "]");
        LOGGER.info("applicationId          = [" + APPLICATION_ID + "]");
        LOGGER.info("autoOffsetResetConfig  = [" + AUTO_OFFSET_RESET_CONFIG + "]");
        LOGGER.info("numStreamThreads       = [" + NUM_STREAM_THREADS + "]");
        LOGGER.info("cacheMaxBytesBuffering = [" + CACHE_MAX_BYTES + "]");
        LOGGER.info("stateDir               = [" + STATE_DIR + "]");

        Properties properties = new Properties();
        
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_STREAM_THREADS);
        properties.put(CACHE_MAX_BYTES_CONFIG, CACHE_MAX_BYTES);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        properties.put("default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        keySerde = new Serdes.StringSerde();
        orderEventSerde = createOrderEventSerde(properties);
        orderInfoEventSerde = createOrderInfoEventSerde(properties);
        facilityInfoEventSerde = createFacilityInfoEventSerde(properties);

        Topology topology = buildTopology();
        LOGGER.info(
                "topologies..."
                        + System.lineSeparator()
                        + System.lineSeparator()
                        + topology.describe().toString().trim()
                        + System.lineSeparator());
        
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

        // re-key order events ("order.placed" and "order.fulfilled") without a key to
        // order events keyed by "order.id"
        streamsBuilder
                .stream(TOPIC, Consumed.with(keySerde, orderEventSerde))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .map((k, v) -> new KeyValue<>(v.orderId, v))
                .to("order-keyed-by-order-id", Produced.with(keySerde, orderEventSerde));

        // aggregate order events ("order.placed" + "order.fulfilled") and
        // output an "order.info" event, keyed by "order.id"
        streamsBuilder
                .stream("order-keyed-by-order-id", Consumed.with(keySerde, orderEventSerde))
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .process(OrderProcessor.supplier())
                .peek((k, v) -> LOGGER.info(String.format("order [%s] = [%s]", k, v)))
                .to("order-info", Produced.with(keySerde, orderInfoEventSerde));

        // re-key "order.info" events by "facility.id"
        streamsBuilder
                .stream("order-info", Consumed.with(keySerde, orderInfoEventSerde))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .map((k, v) -> new KeyValue<>(v.facilityId, v))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .to("order-info-keyed-by-facility-id", Produced.with(keySerde, orderInfoEventSerde));

        // aggregate "order.info" events by "facility.id" with a 1-minute tumbling window
        streamsBuilder
                .stream("order-info-keyed-by-facility-id", Consumed.with(keySerde, orderInfoEventSerde))
                .peek((k, v) -> LOGGER.info(String.format("order.info [%s] = [%s]", k, v)))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofMinutes(1)))
                .aggregate(
                        FacilityInfoEvent::new,
                        (k, v, va) -> {
                            va.processingCount += 1;
                            va.processingMs += v.processingMs;
                            return va;
                        },
                        Materialized.as("facility-info-state-store").with(keySerde, facilityInfoEventSerde))
                .filter((k, v) -> v != null)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                .map((w, v) -> {
                    v.eventTimestamp = w.window().endTime().toEpochMilli();
                    return KeyValue.pair(w.key(), v);
                })
                .peek((k, v) -> LOGGER.info(String.format("facility.info [%s] = [%s]", k, v)))
                .to("facility-info-by-minute", Produced.with(keySerde, facilityInfoEventSerde));

        return streamsBuilder.build();
    }

    private static KafkaJsonSchemaSerde<OrderEvent> createOrderEventSerde(Properties properties) {
        KafkaJsonSchemaSerde<OrderEvent> serde = new KafkaJsonSchemaSerde<>(OrderEvent.class);
        Map<String, Object> serdeConfiguration = new HashMap<>();
        serdeConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        serde.configure(serdeConfiguration, false);
        return serde;
    }

    private static KafkaJsonSchemaSerde<OrderInfoEvent> createOrderInfoEventSerde(Properties properties) {
        KafkaJsonSchemaSerde<OrderInfoEvent> serde = new KafkaJsonSchemaSerde<>(OrderInfoEvent.class);
        Map<String, Object> serdeConfiguration = new HashMap<>();
        serdeConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        serde.configure(serdeConfiguration, false);
        return serde;
    }

    private static KafkaJsonSchemaSerde<FacilityInfoEvent> createFacilityInfoEventSerde(Properties properties) {
        KafkaJsonSchemaSerde<FacilityInfoEvent> serde = new KafkaJsonSchemaSerde<>(FacilityInfoEvent.class);
        Map<String, Object> serdeConfiguration = new HashMap<>();
        serdeConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
        serde.configure(serdeConfiguration, false);
        return serde;
    }
}

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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

/**
 * Processor to aggregate order events ("order.placed" + "order.fulfilled") without a key
 * and output an "order.info" event, keyed by "order.id"
 *
 * Designed to encapsulate the processor and punctuator
 */
public class OrderProcessor implements Processor<String, JsonObject, String, JsonObject> {

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProcessor.class);

    /**
     * State store name
     */
    private final static String STATE_STORE_NAME = "order-state-store";

    /**
     * Processing window milliseconds (i.e. how long to wait for an "order.fulfilled"
     * event to arrive after an "order.placed" event arrives
     */
    private static final int PROCESSING_WINDOW_MILLISECONDS = 86400000; // 24 hours

    /**
     * Processor context
     */
    private ProcessorContext<String, JsonObject> processorContext;

    /**
     * KeyValue store used to store aggregate order event
     */
    private KeyValueStore<String, JsonObject> keyValueStore;

    /**
     * Constructor
     */
    private OrderProcessor() {
        // DO NOTHING
    }

    /**
     * Method to initialize the processor
     *
     * @param processorContext
     */
    @Override
    public void init(ProcessorContext<String, JsonObject> processorContext) {
        this.processorContext = processorContext;
        this.keyValueStore = processorContext.getStateStore(STATE_STORE_NAME);
        this.processorContext.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, new OrderPunctuator(this));
    }

    /**
     * Method to process a Record, aggregating order events
     *
     * @param record
     */
    @Override
    public void process(Record<String, JsonObject> record) {
        String orderId = record.key();
        JsonObject orderEvent = record.value();

        // get the previous aggregate
        JsonObject aggregateOrderEvent = keyValueStore.get(orderId);
        if (aggregateOrderEvent == null) {
            // create a new aggregate order event
            aggregateOrderEvent = new JsonObject();
        }

        // add the order event to the aggregate order event
        String eventType = orderEvent.get("event.type").getAsString();
        switch (eventType) {
            case "order.placed": {
                aggregateOrderEvent.add("order.placed", orderEvent);
                break;
            }
            case "order.fulfilled": {
                aggregateOrderEvent.add("order.fulfilled", orderEvent);
                break;
            }
            default: {
                // ignore other order events
                break;
            }
        }

        // completion criteria
        if (isComplete(aggregateOrderEvent)) {
            JsonObject orderPlacedEvent = aggregateOrderEvent.getAsJsonObject("order.placed");
            JsonObject orderFulfilledEvent = aggregateOrderEvent.getAsJsonObject("order.fulfilled");

            String facilityId = orderFulfilledEvent.get("facility.id").getAsString();

            long orderPlacedTimestamp = orderPlacedEvent.get("event.timestamp").getAsLong();
            long orderFulfilledTimestamp = orderFulfilledEvent.get("event.timestamp").getAsLong();
            long orderProcessingTimeMilliseconds = orderFulfilledTimestamp - orderPlacedTimestamp;

            // build an "order.info" event
            JsonObject orderInfoEvent = new JsonObject();
            orderInfoEvent.addProperty("event.type", "order.info");
            orderInfoEvent.addProperty("facility.id", facilityId);
            orderInfoEvent.addProperty("order.id", orderId);
            orderInfoEvent.addProperty("processing.ms", orderProcessingTimeMilliseconds);

            // forward the "order.info" event to the next processor
            processorContext.forward(new Record<>(orderId, orderInfoEvent, System.currentTimeMillis()));

            // delete the aggregate order event from the state store
            keyValueStore.delete(orderId);
        } else {
            // store the aggregate order event in the state store
            keyValueStore.put(orderId, aggregateOrderEvent);
        }
    }

    /**
     * Method to clean the state store
     *
     * @param timestamp
     */
    private void cleanup(long timestamp) {
        try (KeyValueIterator<String, JsonObject> keyValueIterator = keyValueStore.all()) {
            while (keyValueIterator.hasNext()) {
                KeyValue<String, JsonObject> keyValue = keyValueIterator.next();
                String orderId = keyValue.key;
                JsonObject aggregateOrderEvent = keyValue.value;

                // aggregated order events that are complete (i.e. have an "order.placed"
                // and "order.fulfilled") are handled in the processor

                if (aggregateOrderEvent.has("order.placed")
                        && !aggregateOrderEvent.has("order.fulfilled")) {

                    // aggregated order event is missing the "order.fulfilled" event
                    long eventTimestamp = aggregateOrderEvent
                            .get("order.placed")
                            .getAsJsonObject()
                            .get("event.timestamp")
                            .getAsLong();

                    if (Math.abs(timestamp - eventTimestamp) > PROCESSING_WINDOW_MILLISECONDS) {
                        // we are past the processing window (i.e. an "order.fulfilled" event didn't arrive in time)
                        LOGGER.info(String.format("order.placed without order.fulfilled ... deleting"));
                        // TODO send to topic for unmatched "order.placed" / "order.fulfilled" events
                        keyValueStore.delete(orderId);
                    }
                } else if (!aggregateOrderEvent.has("order.placed")
                        && aggregateOrderEvent.has("order.fulfilled")) {

                    // aggregated order event is missing the "order.placed" event
                    long eventTimestamp = aggregateOrderEvent
                            .get("order.fulfilled")
                            .getAsJsonObject()
                            .get("event.timestamp")
                            .getAsLong();

                    if (Math.abs(timestamp - eventTimestamp) > PROCESSING_WINDOW_MILLISECONDS) {
                        // we are past the processing window (i.e. an "order.fulfilled" event didn't arrive in time)
                        LOGGER.info(String.format("order.fulfilled without order.placed ... deleting"));
                        // TODO send to topic for unmatched "order.placed" / "order.fulfilled" events
                        keyValueStore.delete(orderId);
                    }
                }
            }
        }
    }

    /**
     * Method to check if the aggregate order event is complete
     *
     * @param aggregateOrderEvent
     *
     * @return
     */
    private boolean isComplete(JsonObject aggregateOrderEvent) {
        return aggregateOrderEvent.has("order.placed")
                && aggregateOrderEvent.has("order.fulfilled");
    }

    /**
     * Method to create a ProcessorSupplier to supply the OrderProcessor
     *
     * @return
     */
    public static ProcessorSupplier<String, JsonObject, String, JsonObject> supplier() {
        return new OrderProcessorSupplier();
    }

    /**
     * Punctuator to delegate punctuation to the processor
     */
    private static class OrderPunctuator implements Punctuator {

        /**
         * Processor
         */
        private OrderProcessor processor;

        /**
         * Constructor
         *
         * @param processor
         */
        public OrderPunctuator(OrderProcessor processor) {
            this.processor = processor;
        }

        /**
         * Method to execute when a punctuation occurs
         *
         * @param timestamp
         */
        @Override
        public void punctuate(long timestamp) {
            processor.cleanup(timestamp);
        }
    }

    /**
     * ProcessSupplier to create an OrderProcessor
     */
    private static class OrderProcessorSupplier implements ProcessorSupplier<String, JsonObject, String, JsonObject> {

        /**
         * Method to get (create) an OrderProcessor
         *
         * @return
         */
        @Override
        public Processor<String, JsonObject, String, JsonObject> get() {
            return new OrderProcessor();
        }

        /**
         * Method to get (create) the StoreBuilders (state store builders) for the OrderProcessor
         *
         * @return
         */
        public Set<StoreBuilder<?>> stores() {
            StoreBuilder<?> storeBuilder =
                    Stores
                            .keyValueStoreBuilder(
                                    Stores.persistentKeyValueStore(OrderProcessor.STATE_STORE_NAME),
                                    Serdes.String(),
                                    new JsonObjectSerde());

            return Collections.singleton(storeBuilder);
        }
    }
}

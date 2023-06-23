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
public class OrderProcessor implements Processor<String, OrderEvent, String, OrderInfoEvent> {

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
    private ProcessorContext<String, OrderInfoEvent> processorContext;

    /**
     * KeyValue store used to store aggregate order event
     */
    private KeyValueStore<String, OrderEventAggregate> keyValueStore;

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
    public void init(ProcessorContext<String, OrderInfoEvent> processorContext) {
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
    public void process(Record<String, OrderEvent> record) {
        String orderId = record.key();
        OrderEvent orderEvent = record.value();

        // get the previous aggregate
        OrderEventAggregate orderEventAggregate = keyValueStore.get(orderId);
        if (orderEventAggregate == null) {
            // create a new aggregate order event
            orderEventAggregate = new OrderEventAggregate();
        }

        // add the order event to the aggregate order event
        String eventType = orderEvent.eventType;
        switch (eventType) {
            case "order.placed": {
                orderEventAggregate.orderPlacedEvent = orderEvent;
                break;
            }
            case "order.fulfilled": {
                orderEventAggregate.orderFulfilledEvent = orderEvent;
                break;
            }
            default: {
                // ignore other order events
                break;
            }
        }

        // completion criteria
        if (orderEventAggregate.isComplete()) {
            /*
            LOGGER.info(
                    "OrderEventAggregate for orderId [%s] is complete",
                    orderEventAggregate.orderPlacedEvent.orderId));
             */

            OrderEvent orderPlacedEvent = orderEventAggregate.orderPlacedEvent;
            OrderEvent orderFulfilledEvent = orderEventAggregate.orderFulfilledEvent;

            String facilityId = orderFulfilledEvent.facilityId;

            long orderPlacedTimestamp = orderPlacedEvent.eventTimestamp;
            long orderFulfilledTimestamp = orderFulfilledEvent.eventTimestamp;
            long processingMs = orderFulfilledTimestamp - orderPlacedTimestamp;

            OrderInfoEvent orderInfoEvent =
                    new OrderInfoEvent(System.currentTimeMillis(), orderId, facilityId, processingMs);

            // forward the "order.info" event to the next processor
            processorContext.forward(new Record<>(orderId, orderInfoEvent, System.currentTimeMillis()));

            // delete the aggregate order event from the state store
            keyValueStore.delete(orderId);
        } else {
            /*
            LOGGER.info(
                    "OrderEventAggregate for orderId [%s] is incomplete",
                    orderEventAggregate.orderPlacedEvent.orderId);
            */

            // store the aggregate order event in the state store
            keyValueStore.put(orderId, orderEventAggregate);
        }
    }

    /**
     * Method to clean the state store
     *
     * @param timestamp
     */
    private void cleanup(long timestamp) {
        try (KeyValueIterator<String, OrderEventAggregate> keyValueIterator = keyValueStore.all()) {
            while (keyValueIterator.hasNext()) {
                KeyValue<String, OrderEventAggregate> keyValue = keyValueIterator.next();
                String orderId = keyValue.key;
                OrderEventAggregate orderEventAggregate = keyValue.value;

                // aggregated order events that are complete (i.e. have an "order.placed"
                // and "order.fulfilled") are handled in the processor

                if (!orderEventAggregate.isComplete()) {
                    long eventTimestamp = 0;

                    if (orderEventAggregate.orderPlacedEvent != null) {
                        eventTimestamp = orderEventAggregate.orderPlacedEvent.eventTimestamp;
                    } else {
                        eventTimestamp = orderEventAggregate.orderFulfilledEvent.eventTimestamp;
                    }

                    if (Math.abs(timestamp - eventTimestamp) > PROCESSING_WINDOW_MILLISECONDS) {
                        // we are past the processing window
                        LOGGER.info(String.format("purging incomplete OrderEventAggregate"));
                        // TODO send to topic for unmatched "order.placed" / "order.fulfilled" events
                        keyValueStore.delete(orderId);
                    }
                }
            }
        }
    }

    /**
     * Method to create a ProcessorSupplier to supply the OrderProcessor
     *
     * @return
     */
    public static ProcessorSupplier<String, OrderEvent, String, OrderInfoEvent> supplier() {
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
    private static class OrderProcessorSupplier
            implements ProcessorSupplier<String, OrderEvent, String, OrderInfoEvent> {

        /**
         * Method to get (create) an OrderProcessor
         *
         * @return
         */
        @Override
        public Processor<String, OrderEvent, String, OrderInfoEvent> get() {
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
                                    new OrderEventAggregateSerde());

            return Collections.singleton(storeBuilder);
        }
    }
}

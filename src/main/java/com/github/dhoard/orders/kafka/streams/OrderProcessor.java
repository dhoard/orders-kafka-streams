package com.github.dhoard.orders.kafka.streams;

import com.github.dhoard.orders.kafka.streams.serde.JsonObjectSerde;
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
 * Processor to aggregate order events ("order.placed" + "order.fulfilled") keyed
 * by "order.id" and output an "order.info" event, keyed by "order.id"
 */
public class OrderProcessor implements Processor<String, JsonObject, String, JsonObject> {

    /**
     * Method to return a ProcessorSupplier to create the OrderProcessor
     *
     * @return
     */
    public static ProcessorSupplier<String, JsonObject, String, JsonObject> supplier() {

        return new ProcessorSupplier<>() {
            @Override
            public Processor<String, JsonObject, String, JsonObject> get() {
                return new OrderProcessor();
            }

            public Set<StoreBuilder<?>> stores() {
                StoreBuilder<?> storeBuilder =
                        Stores
                                .keyValueStoreBuilder(
                                        Stores.persistentKeyValueStore(OrderProcessor.STATE_STORE),
                                        Serdes.String(),
                                        new JsonObjectSerde());

                return Collections.singleton(storeBuilder);
            }
        };
    }

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderProcessor.class);

    /**
     * State store name
     */
    public final static String STATE_STORE = "order-state-store";

    /**
     * Maximum processing windows (i.e. how long to wait for an "order.fulfilled"
     * event to arrive after an "order.placed" event arrives
     */
    public static final int PROCESSING_WINDOW_MS = 86400000; // 24 hours

    /**
     * Processor context
     */
    private ProcessorContext<String, JsonObject> processorContext;

    /**
     * KeyValue store used to store order event tuples
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
        this.processorContext.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, new OrderPunctuator(this));
        this.keyValueStore = processorContext.getStateStore(STATE_STORE);
    }

    /**
     * Method to process a Record
     *
     * doesn't handle the scenario where we received an "order.fulfilled" event without an
     * "order.placed" event, or we receive an "order.placed" event without an "order.fulfilled" event
     *
     * should schedule a Punctuator to clean up the state store where we have non-complete events
     *
     * @param record
     */
    @Override
    public void process(Record<String, JsonObject> record) {
        String k = record.key();
        JsonObject v = record.value();

        // get the previous aggregate
        JsonObject aggregate = keyValueStore.get(k);
        if (aggregate == null) {
            // no existing aggregate found, so create one
            aggregate = new JsonObject();
        }

        String eventType = v.get("event.type").getAsString();
        switch (eventType) {
            case "order.placed": {
                aggregate.add("order.placed", v);
                break;
            }
            case "order.fulfilled": {
                aggregate.add("order.fulfilled", v);
                break;
            }
        }

        // completion criteria
        if (aggregate.has("order.placed") && aggregate.has("order.fulfilled")) {
            JsonObject orderPlacedJsonObject = aggregate.getAsJsonObject("order.placed");
            JsonObject orderFulfilledJsonObject = aggregate.getAsJsonObject("order.fulfilled");

            String facilityId = orderFulfilledJsonObject.get("facility.id").getAsString();

            long orderPlacedTimestamp = orderPlacedJsonObject.get("event.timestamp").getAsLong();
            long orderFulfilledTimestamp = orderFulfilledJsonObject.get("event.timestamp").getAsLong();
            long processingMs = orderFulfilledTimestamp - orderPlacedTimestamp;

            // build an "order.info" event
            aggregate = new JsonObject();
            aggregate.addProperty("event.type", "order.info");
            aggregate.addProperty("facility.id", facilityId);
            aggregate.addProperty("order.id", k);
            aggregate.addProperty("processing.ms", processingMs);

            // forward the "order.info" event to the next processor
            processorContext.forward(new Record<>(k, aggregate, System.currentTimeMillis()));

            // delete the aggregate from the state store
            keyValueStore.delete(k);
        } else {
            // store the aggregate in the state store
            keyValueStore.put(k, aggregate);
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
                JsonObject v = keyValue.value;
                if (v.has("order.placed") && !v.has("order.fulfilled")) {
                    // missing order "order.fulfilled"
                    long eventTimestamp = v.get("order.placed").getAsJsonObject().get("event.timestamp").getAsLong();
                    if (Math.abs(timestamp - eventTimestamp) > PROCESSING_WINDOW_MS) {
                        // we are past the processing window (i.e. an "order.fulfilled" event didn't arrive in time)
                        LOGGER.info(String.format("order.placed without order.fulfilled ... deleting"));
                        // TODO send to topic for unmatched "order.placed" and "order.fulfilled" events
                        keyValueStore.delete(keyValue.key);
                    }
                }
            }
        }
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
}

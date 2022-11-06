package com.github.dhoard.orders.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * Composite builder that allow access to both a StreamsBuilder and the StreamsBuilder's internal Topology
 */
public class CompositeBuilder {

    private InnerStreamsBuilder innerStreamsBuilder;

    public CompositeBuilder() {
        innerStreamsBuilder = new InnerStreamsBuilder();
    }

    public StreamsBuilder streamsBuilder() {
        return innerStreamsBuilder.streamsBuilder();
    }

    public Topology topology() {
        return innerStreamsBuilder.topology();
    }

    public Topology build() {
        return innerStreamsBuilder.build();
    }

    private static class InnerStreamsBuilder extends StreamsBuilder {

        public InnerStreamsBuilder() {
            super();
        }

        public StreamsBuilder streamsBuilder() {
            return this;
        }

        public Topology topology() {
            return topology;
        }
    }
}

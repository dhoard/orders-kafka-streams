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

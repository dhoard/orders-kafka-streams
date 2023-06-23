package com.github.dhoard.orders.kafka.streams;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Serde for OrderEventAggregate objects
 */
public class OrderEventAggregateSerde implements Serde<OrderEventAggregate> {

    private static final Serializer<OrderEventAggregate> orderEventAggregateSerializer =
            new OrderEventAggregateSerializer();

    private static final Deserializer<OrderEventAggregate> orderEventAggregateDeserializer =
            new OrderEventAggregateDeserializer();

    @Override
    public Serializer<OrderEventAggregate> serializer() {
        return orderEventAggregateSerializer;
    }

    @Override
    public Deserializer<OrderEventAggregate> deserializer() {
        return orderEventAggregateDeserializer;
    }

    public static class OrderEventAggregateSerializer implements Serializer<OrderEventAggregate> {

        @Override
        public byte[] serialize(String s, OrderEventAggregate orderEventAggregate) {
            ByteArrayOutputStream byteArrayOutputStream = null;
            DataOutputStream dataOutputStream = null;

            try {
                byteArrayOutputStream = new ByteArrayOutputStream();
                dataOutputStream = new DataOutputStream(byteArrayOutputStream);

                OrderEvent orderPlacedEvent = orderEventAggregate.orderPlacedEvent;
                if (orderPlacedEvent == null) {
                    // null marker
                    dataOutputStream.writeByte(0);
                } else {
                    // valid marker
                    dataOutputStream.writeByte(1);
                    dataOutputStream.writeUTF(orderPlacedEvent.eventType);
                    dataOutputStream.writeLong(orderPlacedEvent.eventTimestamp);
                    dataOutputStream.writeUTF(orderPlacedEvent.orderId);
                    dataOutputStream.writeUTF(orderPlacedEvent.facilityId);
                }

                OrderEvent orderFulfilledEvent = orderEventAggregate.orderFulfilledEvent;
                if (orderFulfilledEvent == null) {
                    // null marker
                    dataOutputStream.writeByte(0);
                } else {
                    // valid marker
                    dataOutputStream.writeByte(1);
                    dataOutputStream.writeUTF(orderFulfilledEvent.eventType);
                    dataOutputStream.writeLong(orderFulfilledEvent.eventTimestamp);
                    dataOutputStream.writeUTF(orderFulfilledEvent.orderId);
                    dataOutputStream.writeUTF(orderFulfilledEvent.facilityId);
                }

                dataOutputStream.close();
                byteArrayOutputStream.close();

                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Exception serializing OrderEventAggregate object", e);
            } finally {
                if (dataOutputStream != null) {
                    try {
                        dataOutputStream.close();
                    } catch (Throwable t) {
                        // DO NOTHING
                    }
                }

                if (byteArrayOutputStream != null) {
                    try {
                        byteArrayOutputStream.close();
                    } catch (Throwable t) {
                        // DO NOTHING
                    }
                }
            }
        }
    
        @Override
        public byte[] serialize(String topic, Headers headers, OrderEventAggregate data) {
            throw new UnsupportedOperationException("NOT IMPLEMENTED");
        }
    }

    public static class OrderEventAggregateDeserializer implements Deserializer<OrderEventAggregate> {
            
        @Override
        public OrderEventAggregate deserialize(String s, byte[] bytes) {
            OrderEventAggregate orderEventAggregate = new OrderEventAggregate();
            ByteArrayInputStream byteArrayInputStream = null;
            DataInputStream dataInputStream = null;

            try {
                byteArrayInputStream = new ByteArrayInputStream(bytes);
                dataInputStream = new DataInputStream(byteArrayInputStream);

                // read marker
                byte b = dataInputStream.readByte();
                switch (b) {
                    // valid marker
                    case 1: {
                        String eventType = dataInputStream.readUTF();
                        long eventTimestamp = dataInputStream.readLong();
                        String orderId = dataInputStream.readUTF();
                        String facilityId = dataInputStream.readUTF();
                        orderEventAggregate.orderPlacedEvent =
                                new OrderEvent(eventType, eventTimestamp, orderId, facilityId);
                        break;
                    }
                }

                // read marker
                b = dataInputStream.readByte();
                switch (b) {
                    // valid marker
                    case 1: {
                        String eventType = dataInputStream.readUTF();
                        long eventTimestamp = dataInputStream.readLong();
                        String orderId = dataInputStream.readUTF();
                        String facilityId = dataInputStream.readUTF();
                        orderEventAggregate.orderFulfilledEvent =
                                new OrderEvent(eventType, eventTimestamp, orderId, facilityId);
                        break;
                    }
                }

                return orderEventAggregate;
            } catch (IOException e) {
                throw new RuntimeException("Exception deserializing OrderEventAggregate object", e);
            } finally {
                if (dataInputStream != null) {
                    try {
                        dataInputStream.close();
                    } catch (Throwable t) {
                        // DO NOTHING
                    }
                }

                if (byteArrayInputStream != null) {
                    try {
                        byteArrayInputStream.close();
                    } catch (Throwable t) {
                        // DO NOTHING
                    }
                }
            }
        }
    }
}

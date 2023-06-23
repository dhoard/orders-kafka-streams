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
 * Serde for FacilityInfoEvent objects
 */
public class FaciilityInfoEventSerde implements Serde<FacilityInfoEvent> {

    private static final Serializer<FacilityInfoEvent> FacilityInfoEventSerializer =
            new FacilityInfoEventSerializer();

    private static final Deserializer<FacilityInfoEvent> FacilityInfoEventDeserializer =
            new FacilityInfoEventDeserializer();

    @Override
    public Serializer<FacilityInfoEvent> serializer() {
        return FacilityInfoEventSerializer;
    }

    @Override
    public Deserializer<FacilityInfoEvent> deserializer() {
        return FacilityInfoEventDeserializer;
    }

    public static class FacilityInfoEventSerializer implements Serializer<FacilityInfoEvent> {

        @Override
        public byte[] serialize(String s, FacilityInfoEvent facilityInfoEvent) {
            ByteArrayOutputStream byteArrayOutputStream = null;
            DataOutputStream dataOutputStream = null;

            try {
                byteArrayOutputStream = new ByteArrayOutputStream();
                dataOutputStream = new DataOutputStream(byteArrayOutputStream);

                dataOutputStream.writeUTF(facilityInfoEvent.eventType);
                dataOutputStream.writeLong(facilityInfoEvent.eventTimestamp);
                dataOutputStream.writeUTF(facilityInfoEvent.facilityId);
                dataOutputStream.writeLong(facilityInfoEvent.processingCount);
                dataOutputStream.writeLong(facilityInfoEvent.processingMs);

                dataOutputStream.close();
                byteArrayOutputStream.close();

                return byteArrayOutputStream.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Exception serializing FacilityInfoEvent object", e);
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
        public byte[] serialize(String topic, Headers headers, FacilityInfoEvent data) {
            throw new UnsupportedOperationException("NOT IMPLEMENTED");
        }
    }

    public static class FacilityInfoEventDeserializer implements Deserializer<FacilityInfoEvent> {
            
        @Override
        public FacilityInfoEvent deserialize(String s, byte[] bytes) {
            ByteArrayInputStream byteArrayInputStream = null;
            DataInputStream dataInputStream = null;

            try {
                byteArrayInputStream = new ByteArrayInputStream(bytes);
                dataInputStream = new DataInputStream(byteArrayInputStream);

                long eventTimestamp = dataInputStream.readLong();
                String facilityId = dataInputStream.readUTF();
                long processingCount = dataInputStream.readLong();
                long processingMs = dataInputStream.readLong();

                return new FacilityInfoEvent(eventTimestamp, facilityId, processingCount, processingMs);
            } catch (IOException e) {
                throw new RuntimeException("Exception deserializing FacilityInfoEvent object", e);
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

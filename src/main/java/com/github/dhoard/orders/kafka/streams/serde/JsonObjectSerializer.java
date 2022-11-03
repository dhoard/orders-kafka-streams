package com.github.dhoard.orders.kafka.streams.serde;

import com.google.gson.JsonObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class JsonObjectSerializer implements Serializer<JsonObject> {

    JsonObjectSerializer() {
        // DO NOTHING
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        // DO NOTHING
    }

    public byte[] serialize(String topic, Headers headers, JsonObject data) {
        if (data == null) {
            return null;
        } else {
            try {
                return data.toString().getBytes(StandardCharsets.UTF_8);
            } catch (Throwable t) {
                throw new SerializationException(t);
            }
        }
    }

    public byte[] serialize(String topic, JsonObject data) {
        if (data == null) {
            return null;
        } else {
            try {
                return data.toString().getBytes(StandardCharsets.UTF_8);
            } catch (Throwable t) {
                throw new SerializationException(t);
            }
        }
    }
    public void close() {
        // DO NOTHING
    }
}

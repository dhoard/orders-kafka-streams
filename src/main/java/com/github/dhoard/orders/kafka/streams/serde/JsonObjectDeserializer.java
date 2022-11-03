package com.github.dhoard.orders.kafka.streams.serde;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class JsonObjectDeserializer implements Deserializer<JsonObject> {

    JsonObjectDeserializer() {
        // DO NOTHING
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // DO NOTHING
    }

    @Override
    public JsonObject deserialize(final String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            return JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (Throwable t) {
            throw new SerializationException(t);
        }
    }

    @Override
    public void close() {
        // DO NOTHING
    }
}
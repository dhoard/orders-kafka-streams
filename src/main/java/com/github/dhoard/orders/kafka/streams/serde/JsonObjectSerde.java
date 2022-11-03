package com.github.dhoard.orders.kafka.streams.serde;

import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonObjectSerde implements Serde<JsonObject> {

    private Serializer<JsonObject> serializer;
    private Deserializer<JsonObject> deserializer;

    public JsonObjectSerde() {
        serializer = new JsonObjectSerializer();
        deserializer = new JsonObjectDeserializer();
    }

    @Override
    public void configure(java.util.Map<String, ?> configs, boolean isKey) {
        // DO NOTHING
    }

    @Override
    public Serializer<JsonObject> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<JsonObject> deserializer() {
        return deserializer;
    }
}
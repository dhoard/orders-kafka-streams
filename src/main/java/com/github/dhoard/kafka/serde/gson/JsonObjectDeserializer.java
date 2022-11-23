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

package com.github.dhoard.kafka.serde.gson;

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
    public JsonObject deserialize(final String topic, final byte[] bytes) {
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
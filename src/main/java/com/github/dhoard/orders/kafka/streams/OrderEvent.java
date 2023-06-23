package com.github.dhoard.orders.kafka.streams;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class OrderEvent {

    @JsonProperty
    public String eventType;

    @JsonProperty
    public long eventTimestamp;

    @JsonProperty
    public String orderId;

    @JsonProperty
    public String facilityId;

    public OrderEvent() {
        // DO NOTHING
    }

    public OrderEvent(String eventType, long eventTimestamp, String orderId, String facilityId) {
        this.eventType = eventType;
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.facilityId = facilityId;
    }

    @Override
    public String toString() {
        return "OrderEvent{eventType="
                + eventType
                + ",eventTimestamp="
                + eventTimestamp
                + ",orderId="
                + orderId
                + ",facilityId="
                + facilityId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return eventTimestamp ==
                that.eventTimestamp
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(orderId, that.orderId)
                && Objects.equals(facilityId, that.facilityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTimestamp, orderId, facilityId);
    }
}


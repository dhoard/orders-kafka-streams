package com.github.dhoard.orders.kafka.streams;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class OrderInfoEvent {

    @JsonProperty
    public String eventType;

    @JsonProperty
    public long eventTimestamp;

    @JsonProperty
    public String orderId;

    @JsonProperty
    public String facilityId;

    @JsonProperty
    public long processingMs;

    public OrderInfoEvent() {
        // DO NOTHING
    }

    public OrderInfoEvent(long eventTimestamp, String orderId, String facilityId, long processingMs) {
        this.eventType = "order.info";
        this.eventTimestamp = eventTimestamp;
        this.orderId = orderId;
        this.facilityId = facilityId;
        this.processingMs = processingMs;
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
                + facilityId
                + ",processingMs="
                + processingMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderInfoEvent that = (OrderInfoEvent) o;
        return eventTimestamp ==
                that.eventTimestamp
                && processingMs == that.processingMs
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(orderId, that.orderId)
                && Objects.equals(facilityId, that.facilityId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTimestamp, orderId, facilityId, processingMs);
    }
}


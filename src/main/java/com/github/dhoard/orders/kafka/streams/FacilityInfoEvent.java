package com.github.dhoard.orders.kafka.streams;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class FacilityInfoEvent {

    @JsonProperty
    public String eventType;

    @JsonProperty
    public long eventTimestamp;

    @JsonProperty
    public String facilityId;

    @JsonProperty
    public long processingCount;

    @JsonProperty
    public long processingMs;

    public FacilityInfoEvent() {
        // DO NOTHING
    }

    public FacilityInfoEvent(long eventTimestamp, String facilityId, long processingCount, long processingMs) {
        this.eventType = "facility.info";
        this.eventTimestamp = eventTimestamp;
        this.facilityId = facilityId;
        this.processingCount = processingCount;
        this.processingMs = processingMs;
    }

    @Override
    public String toString() {
        return "FacilityInfoEvent{eventType="
                + eventType
                + ",eventTimestamp="
                + eventTimestamp
                + ",facilityId="
                + facilityId
                + ",processingCount="
                + processingCount
                + ",processingMs="
                + processingMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FacilityInfoEvent that = (FacilityInfoEvent) o;
        return eventTimestamp ==
                that.eventTimestamp
                && processingMs == that.processingMs
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(facilityId, that.facilityId)
                && Objects.equals(processingCount, that.processingCount)
                && Objects.equals(processingMs, that.processingMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, eventTimestamp, facilityId, processingCount, processingMs);
    }
}


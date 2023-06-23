package com.github.dhoard.orders.kafka.streams;

public class OrderEventAggregate {

    public OrderEvent orderPlacedEvent;
    public OrderEvent orderFulfilledEvent;

    public boolean isComplete() {
        return orderPlacedEvent != null && orderFulfilledEvent != null;
    }
}

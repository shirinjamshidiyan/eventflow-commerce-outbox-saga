package com.shirin.order.outbox;

public enum OutboxStatus {
    PENDING, //not sent to kafka yet
    PUBLISHED, // kafka acked it
    FAILED, //failed before, but may be retried
    DEAD //do not retry automatically anymore

}

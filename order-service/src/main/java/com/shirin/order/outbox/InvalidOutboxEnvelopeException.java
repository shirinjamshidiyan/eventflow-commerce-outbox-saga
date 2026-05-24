package com.shirin.order.outbox;

public class InvalidOutboxEnvelopeException extends RuntimeException
{
    public InvalidOutboxEnvelopeException(String message, Throwable cause) {
        super(message, cause);
    }
}

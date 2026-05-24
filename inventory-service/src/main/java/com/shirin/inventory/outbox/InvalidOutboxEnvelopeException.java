package com.shirin.inventory.outbox;

public class InvalidOutboxEnvelopeException extends RuntimeException
{
    public InvalidOutboxEnvelopeException(String message, Throwable cause) {
        super(message, cause);
    }
}

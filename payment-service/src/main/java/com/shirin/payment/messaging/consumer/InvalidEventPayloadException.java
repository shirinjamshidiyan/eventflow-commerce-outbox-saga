package com.shirin.payment.messaging.consumer;

public class InvalidEventPayloadException extends RuntimeException{
    public InvalidEventPayloadException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidEventPayloadException(String message) {
        super(message);
    }
}

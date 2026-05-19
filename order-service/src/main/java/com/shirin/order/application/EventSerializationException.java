package com.shirin.order.application;

public class EventSerializationException extends RuntimeException{
    public EventSerializationException(String message) {
        super(message);
    }

    public EventSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}

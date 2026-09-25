package com.example.orders.service;

public class OrderProcessingException extends RuntimeException {

    public OrderProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}

package com.example.orders.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "orders", uniqueConstraints = @UniqueConstraint(name = "uk_orders_order_id", columnNames = "order_id"))
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    protected Order() {
    }

    public Order(String orderId) {
        this.orderId = orderId;
    }

    public Long getId() {
        return id;
    }

    public String getOrderId() {
        return orderId;
    }
}

package com.example.orders.service;

import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
@Service
public class OrderPersistenceService {

    private final OrderRepository orderRepository;

    public OrderPersistenceService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional(transactionManager = "dbOnlyTM", propagation = Propagation.REQUIRES_NEW)
    public void persistOrder(String orderId) {
        // DB work in its own local transaction.
        orderRepository.save(new Order(orderId));
    }
}

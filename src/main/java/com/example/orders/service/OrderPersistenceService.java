package com.example.orders.service;

import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class OrderPersistenceService {

    private final OrderRepository orderRepository;

    public OrderPersistenceService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @FunctionalInterface
    public interface PublishWork {
        void run();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void publishAndPersist(String orderId, PublishWork publishWork, Runnable afterCommitAction) {
        // DB local transaction starts before publish work.
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // Runs after DB commit has completed.
                afterCommitAction.run();
            }
        });
        publishWork.run();
        orderRepository.save(new Order(orderId));
    }
}

package com.example.orders.service;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

@Service
public class OrderService {

    private static final String NOTIFY_QUEUE_1 = "NOTIFY.QUEUE.1";
    private static final String NOTIFY_QUEUE_2 = "NOTIFY.QUEUE.2";

    private final JmsTemplate publisherJmsTemplate;
    private final OrderPersistenceService orderPersistenceService;

    public OrderService(
            JmsTemplate publisherJmsTemplate,
            OrderPersistenceService orderPersistenceService
    ) {
        this.publisherJmsTemplate = publisherJmsTemplate;
        this.orderPersistenceService = orderPersistenceService;
    }

    @Transactional(transactionManager = "mqOnlyTM", propagation = Propagation.REQUIRES_NEW)
    public void process(String orderId) {
        try {
            // MQPUTs use the transaction-bound publisher session and commit when this method returns.
            publish(NOTIFY_QUEUE_1, orderId);
            publish(NOTIFY_QUEUE_2, orderId);

            // DB local transaction commits inside persistOrder(...) before returning.
            orderPersistenceService.persistOrder(orderId);

        } catch (DataIntegrityViolationException duplicateOrder) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        } catch (Exception exception) {
            throw new OrderProcessingException("Failed to process orderId=" + orderId, exception);
        }
    }

    private void publish(String destination, String orderId) {
        publisherJmsTemplate.convertAndSend(destination, orderId);
    }
}

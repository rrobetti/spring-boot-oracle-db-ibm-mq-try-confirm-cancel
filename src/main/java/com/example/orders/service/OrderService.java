package com.example.orders.service;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.jms.core.JmsTemplate;
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

            // TEST HOOK INVOCATION: beforeDbWork(...) is intentionally a no-op in production and
            // only used by integration tests to inject failures/timing before DB work.
            // DB local transaction commits inside persistOrder(...) before returning.
            beforeDbWork(orderId);
            orderPersistenceService.persistOrder(orderId, () -> onDbCommit(orderId));
            // TEST HOOK INVOCATION: beforePublisherCommit(...) is intentionally a no-op in production and
            // only used by integration tests to inject failures/timing after DB commit and before MQCMIT.
            beforePublisherCommit(orderId);

        } catch (DataIntegrityViolationException duplicateOrder) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        } catch (Exception exception) {
            throw new OrderProcessingException("Failed to process orderId=" + orderId, exception);
        }
    }

    // TEST HOOK (intentional no-op in production):
    // Integration tests use this to inject failures/timing before DB work starts.
    public void beforeDbWork(String orderId) {
    }

    // TEST HOOK (intentional no-op in production):
    // Integration tests use this to inject failures/timing after DB commit and before publisher MQCMIT.
    public void beforePublisherCommit(String orderId) {
    }

    // TEST HOOK (intentional no-op in production):
    // OrderServiceIT.commitOrder_dbBeforeJms spies this method to record the DB afterCommit timestamp
    // and assert DB commit happens before outbound JMS message visibility.
    public void onDbCommit(String orderId) {
    }

    private void publish(String destination, String orderId) {
        publisherJmsTemplate.convertAndSend(destination, orderId);
    }
}

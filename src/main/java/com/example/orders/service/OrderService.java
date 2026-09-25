package com.example.orders.service;

import com.example.orders.repository.OrderRepository;
import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final String NOTIFY_QUEUE_1 = "NOTIFY.QUEUE.1";
    private static final String NOTIFY_QUEUE_2 = "NOTIFY.QUEUE.2";

    private final MQQueueConnectionFactory publisherMqQueueConnectionFactory;
    private final OrderRepository orderRepository;
    private final OrderPersistenceService orderPersistenceService;

    public OrderService(
            MQQueueConnectionFactory publisherMqQueueConnectionFactory,
            OrderRepository orderRepository,
            OrderPersistenceService orderPersistenceService
    ) {
        this.publisherMqQueueConnectionFactory = publisherMqQueueConnectionFactory;
        this.orderRepository = orderRepository;
        this.orderPersistenceService = orderPersistenceService;
    }

    public void process(String orderId) {
        if (orderRepository.existsByOrderId(orderId)) {
            return;
        }

        Connection publisherConnection = null;
        Session publisherSession = null;

        try {
            publisherConnection = publisherMqQueueConnectionFactory.createConnection();
            publisherSession = publisherConnection.createSession(true, Session.SESSION_TRANSACTED);

            publish(publisherSession, NOTIFY_QUEUE_1, orderId);
            publish(publisherSession, NOTIFY_QUEUE_2, orderId);

            beforeDbWork(orderId);
            orderPersistenceService.persistOrder(orderId, () -> onDbCommit(orderId));
            beforePublisherCommit(orderId);

            publisherSession.commit();
        } catch (Exception exception) {
            rollbackQuietly(publisherSession);
            throw new OrderProcessingException("Failed to process orderId=" + orderId, exception);
        } finally {
            closeQuietly(publisherSession);
            closeQuietly(publisherConnection);
        }
    }

    public void beforeDbWork(String orderId) {
    }

    public void beforePublisherCommit(String orderId) {
    }

    public void onDbCommit(String orderId) {
    }

    private void publish(Session session, String destination, String orderId) throws Exception {
        Queue queue = session.createQueue(destination);
        try (MessageProducer producer = session.createProducer(queue)) {
            TextMessage message = session.createTextMessage(orderId);
            producer.send(message);
        }
    }

    private void rollbackQuietly(Session session) {
        if (session == null) {
            return;
        }
        try {
            session.rollback();
        } catch (Exception ignored) {
        }
    }

    private void closeQuietly(Session session) {
        if (session == null) {
            return;
        }
        try {
            session.close();
        } catch (Exception ignored) {
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception ignored) {
        }
    }
}

package com.example.orders;

import com.example.orders.model.Order;
import com.example.orders.repository.OrderRepository;
import com.example.orders.service.OrderService;
import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;

import javax.sql.DataSource;

import java.time.Duration;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

class OrderServiceIT extends BaseIntegrationTest {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JmsListenerEndpointRegistry jmsListenerEndpointRegistry;

    @Autowired
    private DataSource dataSource;

    @SpyBean
    private OrderRepository spyOrderRepository;

    @SpyBean
    private OrderService orderService;

    private MQQueueConnectionFactory directMqConnectionFactory;

    @BeforeEach
    void setUp() throws Exception {
        directMqConnectionFactory = new MQQueueConnectionFactory();
        directMqConnectionFactory.setHostName(mq.getHost());
        directMqConnectionFactory.setPort(mq.getMappedPort(1414));
        directMqConnectionFactory.setQueueManager("QM1");
        directMqConnectionFactory.setChannel(mq.getChannel());
        directMqConnectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        directMqConnectionFactory.setStringProperty(WMQConstants.USERID, "app");
        directMqConnectionFactory.setStringProperty(WMQConstants.PASSWORD, "passw0rd");

        clearQueue("ORDERS.IN");
        clearQueue("NOTIFY.QUEUE.1");
        clearQueue("NOTIFY.QUEUE.2");
        orderRepository.deleteAll();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            mqProxy.setConnectionCut(false);
        } catch (Exception ignored) {
        }
        try {
            oracleProxy.setConnectionCut(false);
        } catch (Exception ignored) {
        }
        removeToxicIfExists("CUT_MQ_DOWN");
        removeToxicIfExists("CUT_MQ_UP");
        removeToxicIfExists("CUT_DB_DOWN");
        removeToxicIfExists("CUT_DB_UP");
        for (MessageListenerContainer container : jmsListenerEndpointRegistry.getListenerContainers()) {
            if (!container.isRunning()) {
                container.start();
            }
        }
    }

    @Test
    void happyPath_dbCommitsAndMessagesBecomeVisible() {
        String orderId = randomOrderId();

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(queueDepth("NOTIFY.QUEUE.1")).isEqualTo(1);
            assertThat(queueDepth("NOTIFY.QUEUE.2")).isEqualTo(1);
            assertThat(orderRepository.existsByOrderId(orderId)).isTrue();
            assertThat(queueDepth("ORDERS.IN")).isEqualTo(0);
        });
    }

    @Test
    void dbFails_messagesRolledBack() {
        String orderId = randomOrderId();

        doThrow(new DataAccessResourceFailureException("forced db failure"))
                .when(spyOrderRepository).save(any(Order.class));

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(queueDepth("NOTIFY.QUEUE.1")).isEqualTo(0);
            assertThat(queueDepth("NOTIFY.QUEUE.2")).isEqualTo(0);
            assertThat(queueDepth("ORDERS.IN")).isEqualTo(1);
            assertThat(orderRepository.existsByOrderId(orderId)).isFalse();
        });
    }

    @Test
    void commitOrder_dbBeforeJms() {
        String orderId = randomOrderId();
        AtomicLong dbCommitTimestamp = new AtomicLong(0L);

        doAnswer(invocation -> {
            dbCommitTimestamp.set(System.currentTimeMillis());
            return invocation.callRealMethod();
        }).when(orderService).onDbCommit(eq(orderId));

        CompletableFuture<Long> jmsVisibleTimestamp = CompletableFuture.supplyAsync(() -> {
            try (Connection connection = directMqConnectionFactory.createConnection()) {
                connection.start();
                try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                    MessageConsumer consumer = session.createConsumer(session.createQueue("NOTIFY.QUEUE.1"));
                    Message message = consumer.receive(TimeUnit.SECONDS.toMillis(10));
                    assertThat(message).isNotNull();
                    return System.currentTimeMillis();
                }
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> dbCommitTimestamp.get() > 0 && jmsVisibleTimestamp.isDone());

        assertThat(dbCommitTimestamp.get()).isLessThanOrEqualTo(jmsVisibleTimestamp.join());
    }

    @Test
    void mqDownDuringPublish_dbRollsBack() throws Exception {
        String orderId = randomOrderId();

        mqProxy.setConnectionCut(true);

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderRepository.existsByOrderId(orderId)).isFalse();
            assertThat(queueDepth("NOTIFY.QUEUE.1")).isEqualTo(0);
            assertThat(queueDepth("NOTIFY.QUEUE.2")).isEqualTo(0);
        });

        mqProxy.setConnectionCut(false);
    }

    @Test
    void mqDownDuringCommit_dbCommits_messageLost_redelivery() {
        String orderId = randomOrderId();

        doAnswer(invocation -> {
            mqProxy.setConnectionCut(true);
            return invocation.callRealMethod();
        }).when(orderService).beforePublisherCommit(eq(orderId));

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderRepository.existsByOrderId(orderId)).isTrue();
            assertThat(queueDepth("NOTIFY.QUEUE.1")).isEqualTo(0);
            assertThat(queueDepth("NOTIFY.QUEUE.2")).isEqualTo(0);
        });

        mqProxy.setConnectionCut(false);
    }

    @Test
    void oracleDownDuringDbWork_messagesRolledBack() {
        String orderId = randomOrderId();

        doThrow(new DataAccessResourceFailureException("oracle connection down"))
                .when(spyOrderRepository)
                .save(argThat(order -> orderId.equals(order.getOrderId())));

        doAnswer(invocation -> {
            oracleProxy.setConnectionCut(true);
            if (dataSource instanceof HikariDataSource hikariDataSource && hikariDataSource.getHikariPoolMXBean() != null) {
                hikariDataSource.getHikariPoolMXBean().softEvictConnections();
            }
            return invocation.callRealMethod();
        }).when(orderService).beforeDbWork(eq(orderId));

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderRepository.existsByOrderId(orderId)).isFalse();
        });

        oracleProxy.setConnectionCut(false);
    }

    @Test
    void idempotency_duplicateMessage() {
        String orderId = randomOrderId();

        sendOrder(orderId);
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(orderRepository.existsByOrderId(orderId)).isTrue());

        sendOrder(orderId);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderRepository.countByOrderId(orderId)).isEqualTo(1);
            assertThat(queueDepthByBody("NOTIFY.QUEUE.1", orderId)).isLessThanOrEqualTo(1);
            assertThat(queueDepthByBody("NOTIFY.QUEUE.2", orderId)).isLessThanOrEqualTo(1);
        });
    }

    @Test
    void idempotency_concurrentProcessOnlyOneNotificationSet() throws Exception {
        String orderId = randomOrderId();

        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            Future<?> first = executor.submit(() -> orderService.process(orderId));
            Future<?> second = executor.submit(() -> orderService.process(orderId));
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        }

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(orderRepository.countByOrderId(orderId)).isEqualTo(1);
            assertThat(queueDepthByBody("NOTIFY.QUEUE.1", orderId)).isLessThanOrEqualTo(1);
            assertThat(queueDepthByBody("NOTIFY.QUEUE.2", orderId)).isLessThanOrEqualTo(1);
        });
    }

    private void sendOrder(String orderId) {
        try (Connection connection = directMqConnectionFactory.createConnection()) {
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Queue queue = session.createQueue("ORDERS.IN");
                try (MessageProducer producer = session.createProducer(queue)) {
                    producer.send(session.createTextMessage(orderId));
                }
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private long queueDepth(String queueName) {
        try (Connection connection = directMqConnectionFactory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                QueueBrowser browser = session.createBrowser(session.createQueue(queueName));
                Enumeration<?> enumeration = browser.getEnumeration();
                long count = 0;
                while (enumeration.hasMoreElements()) {
                    enumeration.nextElement();
                    count++;
                }
                return count;
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private long queueDepthByBody(String queueName, String body) {
        try (Connection connection = directMqConnectionFactory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                QueueBrowser browser = session.createBrowser(session.createQueue(queueName));
                Enumeration<?> enumeration = browser.getEnumeration();
                long count = 0;
                while (enumeration.hasMoreElements()) {
                    Message message = (Message) enumeration.nextElement();
                    if (message instanceof jakarta.jms.TextMessage textMessage && body.equals(textMessage.getText())) {
                        count++;
                    }
                }
                return count;
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private void clearQueue(String queueName) {
        try (Connection connection = directMqConnectionFactory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
                while (consumer.receive(200) != null) {
                    // drain queue
                }
            }
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private String randomOrderId() {
        return "order-" + UUID.randomUUID();
    }

    private void removeToxicIfExists(String name) throws Exception {
        try {
            mqProxy.toxics().get(name).remove();
        } catch (Exception ignored) {
        }
        try {
            oracleProxy.toxics().get(name).remove();
        } catch (Exception ignored) {
        }
    }
}

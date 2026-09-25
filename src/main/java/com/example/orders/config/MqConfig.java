package com.example.orders.config;

import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import jakarta.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.annotation.EnableJms;

@Configuration
@EnableJms
public class MqConfig {

    @Bean
    JmsListenerContainerFactory<?> jmsListenerContainerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setSessionTransacted(true);
        return factory;
    }

    @Bean
    MQQueueConnectionFactory publisherMqQueueConnectionFactory(
            @Value("${ibm.mq.host}") String host,
            @Value("${ibm.mq.port}") int port,
            @Value("${ibm.mq.queue-manager}") String queueManager,
            @Value("${ibm.mq.channel}") String channel,
            @Value("${ibm.mq.user}") String user,
            @Value("${ibm.mq.password}") String password
    ) throws Exception {
        MQQueueConnectionFactory connectionFactory = new MQQueueConnectionFactory();
        connectionFactory.setHostName(host);
        connectionFactory.setPort(port);
        connectionFactory.setQueueManager(queueManager);
        connectionFactory.setChannel(channel);
        connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        connectionFactory.setStringProperty(WMQConstants.USERID, user);
        connectionFactory.setStringProperty(WMQConstants.PASSWORD, password);
        return connectionFactory;
    }
}

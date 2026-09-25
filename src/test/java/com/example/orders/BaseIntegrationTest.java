package com.example.orders;

import com.ibm.mq.testcontainers.MQContainer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

@Testcontainers
@SpringBootTest
public abstract class BaseIntegrationTest {

    static final Network network = Network.newNetwork();

    @Container
    @ServiceConnection
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.26.0-slim-faststart")
            .withUsername("orders")
            .withPassword("orders")
            .withNetwork(network);

    @Container
    static MQContainer mq = new MQContainer(MQContainer.DEFAULT_IMAGE)
            .acceptLicense()
            .withAppUser("app")
            .withAppPassword("passw0rd")
            .withStartupMQSC("99-test-queues.mqsc")
            .withNetwork(network);

    @Container
    static ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")
            .withNetwork(network);

    static ToxiproxyContainer.ContainerProxy mqProxy;
    static ToxiproxyContainer.ContainerProxy oracleProxy;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        if (mqProxy == null) {
            mqProxy = toxiproxy.getProxy(mq, 1414);
        }
        if (oracleProxy == null) {
            oracleProxy = toxiproxy.getProxy(oracle, 1521);
        }

        registry.add("ibm.mq.host", toxiproxy::getHost);
        registry.add("ibm.mq.port", () -> String.valueOf(mqProxy.getProxyPort()));
        registry.add("ibm.mq.queue-manager", () -> "QM1");
        registry.add("ibm.mq.channel", mq::getChannel);
        registry.add("ibm.mq.user", () -> "app");
        registry.add("ibm.mq.password", () -> "passw0rd");
        registry.add("spring.datasource.url", () -> "jdbc:oracle:thin:@"
                + toxiproxy.getHost() + ":" + oracleProxy.getProxyPort() + "/FREEPDB1");
        registry.add("spring.datasource.username", () -> "orders");
        registry.add("spring.datasource.password", () -> "orders");
    }
}

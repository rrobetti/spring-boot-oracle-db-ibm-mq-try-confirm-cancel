package com.example.orders.listener;

import com.example.orders.service.OrderService;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private final OrderService orderService;

    public OrderListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @JmsListener(destination = "ORDERS.IN", containerFactory = "jmsListenerContainerFactory")
    public void onMessage(String orderId) {
        // Listener-session MQ commit (ack of ORDERS.IN) is done by the transacted container
        // only after this method returns successfully.
        orderService.process(orderId);
    }
}

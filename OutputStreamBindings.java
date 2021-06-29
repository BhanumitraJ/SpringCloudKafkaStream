package com.hbi.oms.order.stream;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface OutputStreamBindings {

    String NOTIFICATION_EVENTS = "notification-events";

    @Output(NOTIFICATION_EVENTS)
    MessageChannel notificationEvents();
}

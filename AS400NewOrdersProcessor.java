package com.hbi.oms.order.stream;

import com.hbi.oms.dto.ordertaker.OrderTO;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

import static com.hbi.oms.order.stream.AS400NewOrdersProcessor.As400NewOrdersProcessor.AS400_NEW_ORDERS_PROCESSOR_INPUT;

@EnableBinding(AS400NewOrdersProcessor.As400NewOrdersProcessor.class)
@ConditionalOnProperty(name = "publish-new-orders-to-as400", havingValue = "true")
public class AS400NewOrdersProcessor {

    @StreamListener
    @SendTo(As400NewOrdersProcessor.AS400_NEW_ORDERS_PROCESSOR_OUTPUT)
    public KStream<String, OrderTO> process(@Input(AS400_NEW_ORDERS_PROCESSOR_INPUT) KStream<String, OrderTO> newOrdersStream) {
        return newOrdersStream.filter((key, orderTO) -> "NEW_HORIZON_INTERNATIONAL".equals(orderTO.getSource().getAdditionalProperties().get("SRC_SYSTEM")));
    }

    interface As400NewOrdersProcessor {

        String AS400_NEW_ORDERS_PROCESSOR_INPUT = "new-orders";
        String AS400_NEW_ORDERS_PROCESSOR_OUTPUT = "as400-new-orders";

        @Input(AS400_NEW_ORDERS_PROCESSOR_INPUT)
        KStream as400NewOrdersProcessorInput();

        @Output(AS400_NEW_ORDERS_PROCESSOR_OUTPUT)
        KStream as400NewOrdersProcessorOutput();

    }
}

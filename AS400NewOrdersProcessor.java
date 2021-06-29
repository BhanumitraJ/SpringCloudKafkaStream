package com.hbi.oms.order.stream;

import com.hbi.oms.dto.ordertaker.OrderTO;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

import static com.hbi.oms.order.stream.ThirdPartyNewOrdersProcessor.LegacyNewOrdersProcessor.THIRD_NEW_ORDERS_PROCESSOR_INPUT;

@EnableBinding(ThirdPartyNewOrdersProcessor.LegacyNewOrdersProcessor.class)
@ConditionalOnProperty(name = "publish-new-orders-to-as400", havingValue = "true")
public class ThirdPartyNewOrdersProcessor {

    @StreamListener
    @SendTo(ThirdPartyNewOrdersProcessor.THIRD_PARTY_NEW_ORDERS_PROCESSOR_OUTPUT)
    public KStream<String, OrderTO> process(@Input(THIRD_PARTY_NEW_ORDERS_PROCESSOR_INPUT) KStream<String, OrderTO> newOrdersStream) {
        return newOrdersStream.filter((key, orderTO) -> "NEW_HORIZON_INTERNATIONAL".equals(orderTO.getSource().getAdditionalProperties().get("SRC_SYSTEM")));
    }

    interface LegacyNewOrdersProcessor {

        String THIRD_PARTY_NEW_ORDERS_PROCESSOR_INPUT = "new-orders";
        String THIRD_PARTY_NEW_ORDERS_PROCESSOR_OUTPUT = "legacy-new-orders";

        @Input(THIRD_PARTY_NEW_ORDERS_PROCESSOR_INPUT)
        KStream legacyNewOrdersProcessorInput();

        @Output(THIRD_PARTY_NEW_ORDERS_PROCESSOR_OUTPUT)
        KStream legacyNewOrdersProcessorOutput();

    }
}

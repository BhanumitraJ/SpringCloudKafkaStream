package com.hbi.oms.order.stream;

import com.hbi.oms.dto.statusupdate.OrderStatusUpdateTO;
import com.hbi.oms.dto.statusupdate.StatusValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.EnumSet;

import static com.hbi.oms.dto.statusupdate.StatusValue.ACCEPTED;
import static com.hbi.oms.dto.statusupdate.StatusValue.REJECTED;
import static com.hbi.oms.order.stream.AS400OrderEventsProcessor.As400Processor.AS400_PROCESSOR_INPUT;
import static com.hbi.oms.order.stream.AS400OrderEventsProcessor.As400Processor.AS400_PROCESSOR_OUTPUT;

@EnableBinding(AS400OrderEventsProcessor.As400Processor.class)
public class AS400OrderEventsProcessor {

    public static final EnumSet<StatusValue> VALID_STATUSES = EnumSet.of(ACCEPTED, REJECTED);

    @StreamListener
    @SendTo(AS400_PROCESSOR_OUTPUT)
    public KStream<String, OrderStatusUpdateTO> process(@Input(AS400_PROCESSOR_INPUT) KStream<String, OrderStatusUpdateTO> orderEventsStream) {
        return orderEventsStream.filter((orderId, orderStatusUpdateTO) -> VALID_STATUSES.contains(orderStatusUpdateTO.getStatus().getValue()));
    }

    interface As400Processor {

        String AS400_PROCESSOR_INPUT = "order-events";
        String AS400_PROCESSOR_OUTPUT = "as400-order-events";

        @Input(AS400_PROCESSOR_INPUT)
        KStream as400ProcessorInput();

        @Output(AS400_PROCESSOR_OUTPUT)
        KStream as400ProcessorOutput();

    }
}

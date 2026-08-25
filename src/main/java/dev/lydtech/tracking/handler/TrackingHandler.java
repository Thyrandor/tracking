package dev.lydtech.tracking.handler;

import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.tracking.service.TrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class TrackingHandler {

    private final TrackingService trackingService;

    @KafkaListener(
            id = "trackingConsumerClient",
            topics = "dispatch.tracking",
            groupId = "tracking.dispatch.tracking.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(DispatchPreparing dispatchPreparing) {
        log.info("DispatchTracking event received for order {}", dispatchPreparing.getOrderId());

        try {
            trackingService.process(dispatchPreparing);
        } catch (Exception e) {
            log.error("Processing failure", e);
        }

    }

}

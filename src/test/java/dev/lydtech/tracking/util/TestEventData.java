package dev.lydtech.tracking.util;

import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.tracking.message.TrackingStatusUpdated;

import java.util.UUID;

public class TestEventData {
    public static DispatchPreparing buildDispatchPreparing(UUID orderId) {
        return DispatchPreparing.builder()
                .orderId(orderId)
                .build();
    }

    public static TrackingStatusUpdated buildTrackingStatusUpdated(UUID orderId, TrackingStatusUpdated.Status status) {
        return TrackingStatusUpdated.builder()
                .orderId(orderId)
                .status(status)
                .build();
    }
}

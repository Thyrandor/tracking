package dev.lydtech.tracking.message;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrackingStatusUpdated {
    public enum Status {PREPARING};

    UUID orderId;
    Status status;
}

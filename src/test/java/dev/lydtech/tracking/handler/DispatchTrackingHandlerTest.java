package dev.lydtech.tracking.handler;

import dev.lydtech.dispatch.message.DispatchPreparing;
import dev.lydtech.tracking.service.TrackingService;
import dev.lydtech.tracking.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class DispatchTrackingHandlerTest {

    private DispatchTrackingHandler handler;

    private TrackingService serviceMock;

    @BeforeEach
    void setup() {
        serviceMock = mock(TrackingService.class);
        handler = new DispatchTrackingHandler(serviceMock);
    }

    @Test
    void listen_Success() throws Exception {
        DispatchPreparing testEvent = TestEventData.buildDispatchPreparing(UUID.randomUUID());
        handler.listen(testEvent);
        verify(serviceMock, times(1)).process(testEvent);
    }

    @Test
    void listen_ServiceThrowsException() throws Exception {
        DispatchPreparing testEvent = TestEventData.buildDispatchPreparing(UUID.randomUUID());
        doThrow(new RuntimeException("Service failure")).when(serviceMock).process(testEvent);

        handler.listen(testEvent);

        verify(serviceMock, times(1)).process(testEvent);
    }


}

package org.imsi.queryEREngine.imsi.er.Utilities;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ArrowFlightConnector {
    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
    private static FlightServer server;
    private final Location location;
    private final FlightProducer producer;


    public ArrowFlightConnector(ArrowDataHandler arrowHandler, int port) {
        this.producer = new ArrowFlightProducer(arrowHandler, ALLOCATOR);
        this.location = Location.forGrpcInsecure("0.0.0.0", port);
    }

    public void start() throws Exception{
        System.out.println("Attempting server creation...");
        server = FlightServer.builder(ALLOCATOR, this.location, this.producer).build();
        System.out.println("Server initialized - Attempting start...");
        server.start();
        System.out.println("Server listening on port " + server.getPort());
        server.awaitTermination();
    }

    public static void stop() throws InterruptedException {
        if (server != null) {
            server.close();
            server = null;
        }
    }
}

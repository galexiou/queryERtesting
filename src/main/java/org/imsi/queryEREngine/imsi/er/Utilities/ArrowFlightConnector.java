package org.imsi.queryEREngine.imsi.er.Utilities;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ArrowFlightConnector {
    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
    private static FlightClient client;
    private final Location location;
    private static final int FLIGHT_TIMEOUT = 2;


    public ArrowFlightConnector(int port) {
        this.location = Location.forGrpcInsecure("0.0.0.0", port);
        System.out.println("Attempting to connect to server at port " + port + "...");
        client = FlightClient.builder(ALLOCATOR, this.location).build();
        System.out.println("Connection established");
    }

    public void putData(VectorSchemaRoot vsr, String descriptor){
        FlightClient.ClientStreamListener listener = client.startPut(
                FlightDescriptor.path(descriptor),
                vsr, new AsyncPutListener());
        listener.putNext();
        listener.completed();
        listener.getResult();
    }

    public VectorSchemaRoot getData(String descriptor){
        FlightStream flightStream = client.getStream(new Ticket(
                FlightDescriptor.path(descriptor).getPath()
                        .get(0).getBytes(StandardCharsets.UTF_8)));
        int batch = 0;
        VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot();
        while (flightStream.next()) {
            batch++;
//            System.out.println("Client Received batch #" + batch + ", Data:");
//            System.out.println(vectorSchemaRootReceived.getFieldVectors());
//            System.out.println(flightStream.getSchema());
        }
        return vectorSchemaRootReceived;

    }

    public boolean isPredictionReady(){
        Iterator<Result> actionResult = client.doAction(new Action("is_ready",
                FlightDescriptor.path("null").getPath().get(0).getBytes(StandardCharsets.UTF_8)),
                CallOptions.timeout(FLIGHT_TIMEOUT, TimeUnit.SECONDS));
        if (actionResult.hasNext()) {
            Result result = actionResult.next();
            String result_body = new String(result.getBody(), StandardCharsets.UTF_8);
            return result_body.equals("yes");
        }
        return false;

    }
    public void doAction(String actionType, String descriptor){
        Iterator<Result> actionResult = client.doAction(new Action(actionType,
                FlightDescriptor.path(descriptor).getPath().get(0).getBytes(StandardCharsets.UTF_8)));
        while (actionResult.hasNext()) {
            Result result = actionResult.next();
            System.out.println(new String(result.getBody(), StandardCharsets.UTF_8));
        }

    }
}

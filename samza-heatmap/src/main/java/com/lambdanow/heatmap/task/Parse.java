package com.lambdanow.heatmap.task;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class Parse implements StreamTask {
	  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "pointss");
	  // @SuppressWarnings("unchecked")
	  @Override
	  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        GenericRecord event = (GenericRecord) envelope.getMessage();
        
        String view = event.get("view").toString();
        // long timestamp = (long) event.get("timestamp");
        // int userId = (int) event.get("userId");
        
        int x = (int) event.get("x");
        int xMax = (int) event.get("xMax");
        int y = (int) event.get("y");
        int yMax = (int) event.get("yMax");
        
        int normX = 800;
        int normY = 600;
        
        String point = view + "," + Integer.toString(x * normX / xMax) + "," +  Integer.toString(y * normY / yMax);
        System.out.println("PARSE: " + point);
	    try {
	    	
	      collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, point, "1"));
	    } catch (Exception e) {
	      System.err.println("Unable to parse line: " + event);
	    }
	  }
}

package com.lambdanow.heatmap.task;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;


public class Analyse implements StreamTask {
	  // private KeyValueStore<String, String> store;
/*
	  @SuppressWarnings("unchecked")
	  public void init(Config config, TaskContext context) {
		    System.out.println("init store");
		    store = (KeyValueStore<String, String>) context.getStore("points-store");
		    System.out.println("store initialized");
	  }
*/	  
	  
	 @Override
	 public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	        GenericRecord event = (GenericRecord) envelope.getMessage();
			
	        String key = (String) event.get("key");
	        int count = (int) event.get("value");
	        
	        // String key = (String) envelope.getKey();
			// String value = (String) envelope.getMessage();
		 
	        // String key = (String) envelope.getKey();
	        // int value = (int) envelope.getMessage();
	        
	        // Integer count = store.get(key);
	        // if ( count == null ) count = 0;
	        // store.put(key, count + Integer.parseInt(value));
	      
	        System.out.println("-----------------------------------");
	        System.out.println("ANALYSE: key: " + key + " val " + Integer.toString(count));
	        }
}
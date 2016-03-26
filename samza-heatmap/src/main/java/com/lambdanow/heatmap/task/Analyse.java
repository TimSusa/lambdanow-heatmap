package com.lambdanow.heatmap.task;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;


public class Analyse implements StreamTask, InitableTask {
	  private KeyValueStore<String, Integer> store;

	  @SuppressWarnings("unchecked")
	  public void init(Config config, TaskContext context) {
		    store = (KeyValueStore<String, Integer>) context.getStore("points");
	  }
	  
	  
	 @Override
	 public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	        // GenericRecord event = (GenericRecord) envelope.getMessage();
	        
	        String key = (String) envelope.getKey();
	        int value = (int) envelope.getMessage();
	        
	        Integer count = store.get(key);
	        if ( count == null ) count = 0;
	        store.put(key, count + value);
	      
	        // System.out.println("-----------------------------------");
	        // System.out.println("ANALYSE: key: " + key + " and value: " + Integer.toString(count));
	        }
}
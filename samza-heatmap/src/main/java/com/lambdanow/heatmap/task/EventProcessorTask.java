package com.lambdanow.heatmap.task;

import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
// import org.apache.samza.config.Config;
// import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

final class HeatmapPoint {
	public int radius;
	public int value;
	public int x;
	public int y;
}

final class Heatmap {
	public String view;
	public Set<Integer> knownUserIds = new HashSet<Integer>();
	public List<HeatmapPoint> heatmapData = new ArrayList<HeatmapPoint>();
	/*
	 * We have to override that stuff to get in between class comparison to work.
	 * 
	 * */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(37, 43). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(view).
            toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
       if (!(obj instanceof Heatmap))
            return false;
        if (obj == this)
            return true;

        Heatmap rhs = (Heatmap) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(view, rhs.view).
            isEquals();
    }
}

final class Heatmaps {
	public Set<Heatmap> data= new HashSet<Heatmap>();
}

final class MyCounts {
	public String view;
	public int x;
	public int y;
	
	/*
	 * We have to override that stuff to get containKey to work with this.
	 * 
	 * */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(view).
            append(x).
            append(y).
            toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
       if (!(obj instanceof MyCounts))
            return false;
        if (obj == this)
            return true;

        MyCounts rhs = (MyCounts) obj;
        return new EqualsBuilder().
            // if deriving: appendSuper(super.equals(obj)).
            append(view, rhs.view).
            append(x, rhs.x).
            append(y, rhs.y).
            isEquals();
    }
}

public class EventProcessorTask implements StreamTask {
	  private Map<MyCounts, Integer> counts = new HashMap<MyCounts, Integer>();
	  public Heatmaps heatmaps = new Heatmaps();
	  public Heatmap heatmap = new Heatmap();
	  
	  // Norm Values
	  int xNormMax = 800;
	  int yNormMax = 600;
  
	 @Override
	    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	        GenericRecord event = (GenericRecord) envelope.getMessage();
	        
	        String view = event.get("view").toString();
	        long timestamp = (long) event.get("timestamp");
	        int userId = (int) event.get("userId");
	        
	        int x = (int) event.get("x");
	        int xMax = (int) event.get("xMax");
	        int y = (int) event.get("y");
	        int yMax = (int) event.get("yMax");
	        
	        System.out.println("");
	        System.out.println("-----------------------------------");
	        System.out.println("view: " + view);
	        System.out.println("timestamp " + timestamp);
	        System.out.println("userId " + userId);
	        System.out.println("x " + x);
	        System.out.println("xMax " + xMax);
	        System.out.println("y " + y);
	        System.out.println("yMax " + yMax);
	        
	        // Here we try to count points
	        MyCounts myCount = new MyCounts();
	        myCount.view = view;
	        myCount.x = (x * xNormMax) / xMax;
	        myCount.y = (y * yNormMax) / yMax;
	        
	        if ( counts.containsKey(myCount)) {
	        	int newCount = counts.get(myCount) + 1;
	        	counts.put(myCount, newCount);
	        	System.out.println("View:  " + myCount.view + " with count=" + newCount + " for specific point: " + Integer.toString(myCount.x) + "/" + Integer.toString(myCount.y));
	        } else {
	        	System.out.println("creating new entry for view: " + myCount.view + " with specific point: " + myCount.x + "/" + myCount.y);
	        	counts.put(myCount, 0);
	        }
	        
	        // Here we try to pack the overview of heatmaps, requestable via client
	        // Set heatmap point for one view
	        HeatmapPoint heatmapPoint = new HeatmapPoint();
	        heatmapPoint.x = (x * xNormMax) / xMax;
	        heatmapPoint.y = (y * yNormMax) / yMax;
	        heatmapPoint.radius = 10;
	        heatmapPoint.value = 1;
	        
	        // Update heatmap
	        heatmap.view = view;
	        
	        // THis is some kind of vehicle...
	        // If user is known, update this data grid, otherwise add new one
	        if ( heatmap.knownUserIds.contains(userId) ) {
	        	// Extract index
	        	Iterator<Integer> iter = heatmap.knownUserIds.iterator();
	        	int foundIdx = 0;
	        	int cntIdx = 0;
	    		while (iter.hasNext()) {
	    			int tmp = iter.next();
	    			if (tmp==userId) {
	    				foundIdx = cntIdx;
	    			}
	    			cntIdx = cntIdx + 1;
	    		}
	     
	        	// Update Data grid
	        	System.out.println("Update Data grid for userid: " + Integer.toString(userId));
	        	heatmap.heatmapData.set(foundIdx, heatmapPoint);
	        	
	        } else {
	        	// Create new data grid
	        	System.out.println("Create new data grid for userID: " + Integer.toString(userId));
	        	heatmap.heatmapData.add(heatmapPoint);
	        }
	        heatmap.knownUserIds.add(userId);
	        System.out.println("heatmap data size is: " + Integer.toString(heatmap.heatmapData.size()));
	        
	        // Update set of heatmaps
	        heatmaps.data.add(heatmap);
	        System.out.println("Update set of heatmaps with size: " + Integer.toString(heatmaps.data.size()));	        	        	       
	    }
}
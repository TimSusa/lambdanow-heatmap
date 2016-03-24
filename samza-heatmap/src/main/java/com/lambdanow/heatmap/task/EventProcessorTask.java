package com.lambdanow.heatmap.task;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
// import org.apache.samza.config.Config;
// import org.apache.samza.storage.kv.KeyValueStore;
// import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
// import org.apache.samza.system.OutgoingMessageEnvelope;
// import org.apache.samza.system.SystemStream;
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
        return new HashCodeBuilder(31, 17). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(view).
            // append(userId).
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
            // append(userId, rhs.userId).
            isEquals();
    }
}

final class Heatmaps {
	public Set<Heatmap> data= new HashSet<Heatmap>();
}

final class MyCounts {
	public String view;
	public int userId;
	
	/*
	 * We have to override that stuff to get containKey to work with this.
	 * 
	 * */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(view).
            append(userId).
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
            append(userId, rhs.userId).
            isEquals();
    }
}

// import java.util.concurrent.TimeUnit;
// public class EventProcessorTask implements StreamTask, InitableTask, WindowableTask {
public class EventProcessorTask implements StreamTask {
	  // private int edits = 0;
	  // private int byteDiff = 0;
	  // private Set<int> titles = new HashSet<int>();
	  private Map<MyCounts, Integer> counts = new HashMap<MyCounts, Integer>();
	  public Heatmaps heatmaps = new Heatmaps();
	  public Heatmap heatmap = new Heatmap();

	  // private KeyValueStore<String, Integer> store;
/*
	  public void init(Config config, TaskContext context) {
	    this.store = (KeyValueStore<String, Integer>) context.getStore("newheatmapchannel2");
	  }
*/
	  // @SuppressWarnings("unchecked")
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
	        
	        MyCounts myCount = new MyCounts();
	        myCount.view = view;
	        myCount.userId = userId;
	        
	        if ( counts.containsKey(myCount)) {
	        	int newCount = counts.get(myCount) + 1;
	        	counts.put(myCount, newCount);
	        	System.out.println("View count:  " + newCount + " with userId: " + Integer.toString(myCount.userId));
	        } else {
	        	counts.put(myCount, 0);
	        }
	        
	        // Set heatmap point for one view
	        HeatmapPoint heatmapPoint = new HeatmapPoint();
	        heatmapPoint.x = x;
	        heatmapPoint.y = y;
	        heatmapPoint.radius = 10;
	        heatmapPoint.value = 1;
	        
	        // Update heatmap
	        heatmap.view = view;
	        
	        // If user is known, update this data grid, otherwise add new one
	        if ( heatmap.knownUserIds.contains(userId) ) {
	        	// Extract index
	        	/*
	        	Object[] userIdArray = heatmap.knownUserIds.toArray();
	        	int idx = 0;
	        	int cnt = 0;
	        	for(Object tmp: userIdArray){
	        		if ( (int)tmp == userId ) {
	        			idx = cnt;
	        		}
	        		cnt = cnt + 1;
	        	}
	        	*/
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
	        	        
	        // Map<String, Object> edit = (Map<String, Object>) envelope.getMessage();
	        // Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
/*
	        Integer editsAllTime = store.get("count-edits-all-time");
	        if (editsAllTime == null) editsAllTime = 0;
	        store.put("count-edits-all-time", editsAllTime + 1);
*/
	        // edits += 1;
	        // titles.add((String) view);
	        // byteDiff += (Integer) edit.get("x");
/*
	        for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
	          if (Boolean.TRUE.equals(flag.getValue())) {
	            Integer count = counts.get(flag.getKey());

	            if (count == null) {
	              count = 0;
	            }

	            count += 1;
	            counts.put(flag.getKey(), count);
	          }
	        }	      
	        */  
	        
	        

	        	        
	        // partitioned by view
	        // collector.send(countStream, view, newCount);
	    }
/*
	  @Override
	  public void window(MessageCollector collector, TaskCoordinator coordinator) {
	    counts.put("edits", edits);
	    counts.put("bytes-added", byteDiff);
	    counts.put("unique-titles", titles.size());
	    counts.put("edits-all-time", store.get("count-edits-all-time"));

	    collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "newheatmapchannel2"), counts));

	    // Reset counts after windowing.
	    edits = 0;
	    byteDiff = 0;
	    titles = new HashSet<String>();
	    counts = new HashMap<String, Integer>();
	  }
	  */
}
package com.lambdanow.heatmap.task;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;


final class HeatmapPoint {
    public int radius;
    public int value;
    public int x;
    public int y;
    public String view;
    public int hashCode;
}

final class MyCounts {
    public String view;
    public int x;
    public int y;

    /*
     * We have to override that stuff to get containKey to work with this.
     * 
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
        // if deriving: appendSuper(super.hashCode()).
                append(view).append(x).append(y).toHashCode();
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
                append(view, rhs.view).append(x, rhs.x).append(y, rhs.y).isEquals();
    }
}

public class EventProcessorTask implements StreamTask, InitableTask {
    
    private static final String MONGO_HOST = "task.mongo.host";
    private static final String MONGO_PORT = "task.mongo.port";
    private static final String MONGO_DB_NAME = "task.mongo.db";
    private static final String MONGO_COLLECTION = "task.mongo.collection";
    private String mongoCollection = "";
    MongoClient mongoClient;
    MongoDatabase db;

    private Map<MyCounts, Integer> counts = new HashMap<MyCounts, Integer>();

    int xNormMax = 800;
    int yNormMax = 600;
    int pointRateMax = 100;

    public void init(Config config, TaskContext context) throws ParseException {
        System.out.println("Init MongoDB");
        mongoCollection = config.get(MONGO_COLLECTION);
        mongoClient = new MongoClient(config.get(MONGO_HOST), Integer.parseInt(config.get(MONGO_PORT)));
        db = mongoClient.getDatabase(config.get(MONGO_DB_NAME));
        System.out.println("Initialized MongoDB");
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
            throws ParseException {
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

        // Count points
        MyCounts myCount = new MyCounts();
        myCount.view = view;

        // Avoid dividing through zero
        if (xMax == 0) {
            xMax = 1;
        }
        if (yMax == 0) {
            yMax = 1;
        }
        // Quantize
        myCount.x = (x * xNormMax) / xMax;
        myCount.y = (y * yNormMax) / yMax;
        
        // Count
        if (counts.containsKey(myCount)) {
            int newCount = counts.get(myCount) + 1;
            counts.put(myCount, newCount);

            // Set local maximum
            if (pointRateMax < newCount) {
                pointRateMax = newCount;
            }
            System.out.println("View:  " + myCount.view + " with count=" + newCount + " for specific point: "
                    + Integer.toString(myCount.x) + "/" + Integer.toString(myCount.y));
        } else {
            System.out.println("creating new entry for view: " + myCount.view + " with specific point: " + myCount.x
                    + "/" + myCount.y);
            counts.put(myCount, 1);
        }

        // Pack and send
        HeatmapPoint heatmapPoint = new HeatmapPoint();
        if (pointRateMax == 0) {
            pointRateMax = 1;
        }
        heatmapPoint.x = myCount.x;
        heatmapPoint.y = myCount.y;
        heatmapPoint.radius = counts.get(myCount) * 100 / pointRateMax;
        heatmapPoint.value = counts.get(myCount) * 100 / pointRateMax;
        heatmapPoint.view = view;
        heatmapPoint.hashCode = myCount.hashCode();
             
        // Create db entry for new value,
        // Update otherwise
        if (counts.get(myCount) <= 1) {
            insertToMongoDb(heatmapPoint);
        } else {
            updateToMongoDb(heatmapPoint);
        }
    }

    private MongoCollection<Document> getCollection() {
        if (mongoCollection.isEmpty()) {
            System.out.println("ERROR: Mongocollection name is empty!");
        }
        return db.getCollection(this.mongoCollection);
    }
    
    private void insertToMongoDb(HeatmapPoint point) {
        System.out.println("insertToMongoDb(): ");
        this.getCollection()
                .insertOne(new Document("point", new Document().append("view", point.view).append("x", point.x)
                        .append("y", point.y).append("radius", point.radius).append("weight", point.value)).append("hashCode", point.hashCode));
        System.out.println("point inserted");
    }
    
    private void updateToMongoDb(HeatmapPoint point) {
        System.out.println("updateToMongoDb(): " + Integer.toString(point.value));
        
        Bson filter = new Document("hashCode", point.hashCode);
        Bson updateRadius = Updates.set("point.radius", point.radius);
        Bson updateWeight = Updates.set("point.weight", point.value);
        Bson updates = Updates.combine(updateRadius, updateWeight);
        UpdateResult res = this.getCollection().updateOne(filter, updates);
        if (res.wasAcknowledged()) {
            System.out.println("Updating point... acknoledged");
        } else {
            System.out.println("Updating point... NOT acknoledged");
        }
    }
}
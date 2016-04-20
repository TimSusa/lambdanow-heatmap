package com.lambdanow.heatmap.task;

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
import org.apache.samza.task.WindowableTask;
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
    public int xx;
    public int yy;
    public String view;
    public int hashCode;

    public HeatmapPoint(int radius, int value, int xx, int yy, String view, int hashCode) {
        this.radius = radius;
        this.value = value;
        this.xx = xx;
        this.yy = yy;
        this.view = view;
        this.hashCode = hashCode;
    }
}

final class CountPoint {
    public String view;
    public int xx;
    public int yy;
    public long timestamp;

    public CountPoint(String view, int xx, int yy, long timestamp) {
        this.view = view;
        this.xx = xx;
        this.yy = yy;
        this.timestamp = timestamp;
    }
    
    /*
     * We have to override that stuff to get containKey to work with this.
     * 
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
        // if deriving: appendSuper(super.hashCode()).
                append(view).append(xx).append(yy).toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CountPoint)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        CountPoint rhs = (CountPoint) obj;
        return new EqualsBuilder()
                // if deriving: appendSuper(super.equals(obj)).
                .append(view, rhs.view).append(xx, rhs.xx).append(yy, rhs.yy).isEquals();
    }
}

/**
 * EventProcessorTask.
 * 
 */
public class EventProcessorTask implements StreamTask, InitableTask, WindowableTask {

    private static final String MONGO_HOST = "task.mongo.host";
    private static final String MONGO_PORT = "task.mongo.port";
    private static final String MONGO_DB_NAME = "task.mongo.db";
    private static final String MONGO_COLLECTION = "task.mongo.collection";
    private String mongoCollection = "";
    MongoClient mongoClient;
    MongoDatabase db;

    private Map<CountPoint, Integer> counts = new HashMap<CountPoint, Integer>();

    private static int xNormMax = 800;
    private static int yNormMax = 600;
    int pointRateMax = 100;
    long firstTimestamp = 0;
    private static long maxTimeDiff = 60000; // milisec

    /**
     * init.
     * 
     */
    public void init(Config config, TaskContext context) {
        System.out.println("Init MongoDB");
        mongoCollection = config.get(MONGO_COLLECTION);
        mongoClient = new MongoClient(config.get(MONGO_HOST), Integer.parseInt(config.get(MONGO_PORT)));
        db = mongoClient.getDatabase(config.get(MONGO_DB_NAME));
        System.out.println("Initialized MongoDB");

        firstTimestamp = System.currentTimeMillis();
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println("window: --------------------------------------->");
        if (!counts.isEmpty()) {
            for (CountPoint key : counts.keySet()) {
                long timeDiff = (long) key.timestamp - firstTimestamp;

                // "Afterglow"
                if (timeDiff >= maxTimeDiff) {
                    // scale counts, reduce the rate
                    //int newCount = (int)(counts.get(key) * 0.75 );
                    int newCount = counts.get(key) / 2;
                    
                    key.timestamp = System.currentTimeMillis();
                    counts.put(key, newCount);

                    if (pointRateMax == 0) {
                        pointRateMax = 1;
                    }
                    // Avoid old points with radius 1 and val 1
                    /*
                    if (newCount == 1) {
                        newCount = 0;
                    }
                    */
                    int radius = newCount * 100 / pointRateMax;
                    int value = newCount * 100 / pointRateMax;
                    updateToMongoDb(new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode()));
                    System.out.println("Old Point: " + key.xx + " / " + key.yy + " in view " + key.view + " updated! " + timeDiff);
                }
            }
        } else {
            System.out.println("window: ----STORE IS EMPTY!--------------------------->");
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        GenericRecord event = (GenericRecord) envelope.getMessage();

        String view = event.get("view").toString();
        long timestamp = System.currentTimeMillis(); // (long) event.get("timestamp");
        int xx = (int) event.get("x");
        int xxMax = (int) event.get("xMax");
        int yy = (int) event.get("y");
        int yyMax = (int) event.get("yMax");

        // Temp hack: Ignore points not coming from "/"
        if (view.substring(1).isEmpty()) {

            System.out.println("-----------------------------------");
            System.out.println("timestamp " + timestamp);
            System.out.println("x " + xx);
            System.out.println("xMax " + xxMax);
            System.out.println("y " + yy);
            System.out.println("yMax " + yyMax);


            // Avoid dividing through zero
            if (xxMax == 0) {
                xxMax = 1;
            }
            if (yyMax == 0) {
                yyMax = 1;
            }
            
            // Quantize
            int newXx = (xx * xNormMax) / xxMax;
            int newYy = (yy * yNormMax) / yyMax;

            // Set count point
            CountPoint countPoint = new CountPoint(view, newXx, newYy, timestamp);

            // Count
            if (counts.containsKey(countPoint)) {
                int newCount = counts.get(countPoint) + 1;
                counts.put(countPoint, newCount);

                // Set local maximum
                if (pointRateMax < newCount) {
                    pointRateMax = newCount;
                }

                if (pointRateMax == 0) {
                    pointRateMax = 1;
                }

                int radius = counts.get(countPoint) * 100 / pointRateMax;
                int value = counts.get(countPoint) * 100 / pointRateMax;
                updateToMongoDb(new HeatmapPoint(radius, value, countPoint.xx, countPoint.yy, countPoint.view,
                        countPoint.hashCode()));

                System.out.println("View:  " + countPoint.view + " with count=" + newCount + " for specific point: "
                        + Integer.toString(countPoint.xx) + "/" + Integer.toString(countPoint.yy));
            } else {
                counts.put(countPoint, 1);
                insertToMongoDb(
                        new HeatmapPoint(1, 1, countPoint.xx, countPoint.yy, countPoint.view, countPoint.hashCode()));
                System.out.println("creating new entry for view: " + countPoint.view + " with specific point: "
                        + countPoint.xx + "/" + countPoint.yy);
            }
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
                .insertOne(new Document("point",
                        new Document().append("view", point.view).append("x", point.xx).append("y", point.yy)
                                .append("radius", point.radius).append("weight", point.value)).append("hashCode",
                                        point.hashCode));
        System.out.println("point inserted");
    }

    private void updateToMongoDb(HeatmapPoint point) {
        // System.out.println("updateToMongoDb(): " + Integer.toString(point.value));

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
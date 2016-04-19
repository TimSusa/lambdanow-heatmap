package com.lambdanow.heatmap.task;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        if (!(obj instanceof CountPoint))
            return false;
        if (obj == this)
            return true;

        CountPoint rhs = (CountPoint) obj;
        return new EqualsBuilder().
        // if deriving: appendSuper(super.equals(obj)).
                append(view, rhs.view).append(xx, rhs.xx).append(yy, rhs.yy).isEquals();
    }
}

public class EventProcessorTask implements StreamTask, InitableTask, WindowableTask {

    private static final String MONGO_HOST = "task.mongo.host";
    private static final String MONGO_PORT = "task.mongo.port";
    private static final String MONGO_DB_NAME = "task.mongo.db";
    private static final String MONGO_COLLECTION = "task.mongo.collection";
    private String mongoCollection = "";
    MongoClient mongoClient;
    MongoDatabase db;

    private Map<CountPoint, Integer> counts = new HashMap<CountPoint, Integer>();

    int xNormMax = 800;
    int yNormMax = 600;
    int pointRateMax = 100;
    String firstTimestamp = "";
    long maxTimeDiff = 60; // sec

    public void init(Config config, TaskContext context) throws ParseException {
        System.out.println("Init MongoDB");
        mongoCollection = config.get(MONGO_COLLECTION);
        mongoClient = new MongoClient(config.get(MONGO_HOST), Integer.parseInt(config.get(MONGO_PORT)));
        db = mongoClient.getDatabase(config.get(MONGO_DB_NAME));
        System.out.println("Initialized MongoDB");

        firstTimestamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        System.out.println("window: --------------------------------------->");
        if (!counts.isEmpty()) {
            for (CountPoint key : counts.keySet()) {

                String time1 = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(key.timestamp * 1000); // multiply by
                                                                                                         // 1000
                                                                                                         // to get back
                                                                                                         // unixtimestamp
                                                                                                         // info
                long timeDiff = calcTimeDiff(firstTimestamp, time1); // past, earlier
                int minutes = (int) (timeDiff / (1000 * 60) % 60);
                int seconds = (int) (timeDiff / 1000 % 60);
                int rangeInSeconds = (int) minutes * 60 + seconds;

                // System.out.println("diff: " + rangeInSeconds + "sec and prev timestamp " + time1);

                // "Afterglow"
                if (rangeInSeconds >= maxTimeDiff) {
                    // scale counts, reduce the rate
                    int newCount = counts.get(key) * 3 / 4;
                    counts.put(key, newCount);
                    if (pointRateMax == 0) {
                        pointRateMax = 1;
                    }
                    // Avoid old points with radius 1 and val 1
                    if (newCount == 1) {
                        newCount = 0;
                    }
                    int radius = newCount * 100 / pointRateMax;
                    int value = newCount * 100 / pointRateMax;
                    updateToMongoDb(new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode()));
                    System.out.println("Old Point: " + key.xx + " / " + key.yy + " in view " + key.view + " updated!: "
                            + rangeInSeconds + " and prev timestamp " + time1);
                }
            }
        } else {
            System.out.println("window: ----STORE IS EMPTY!--------------------------->");
        }
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
            throws ParseException {
        GenericRecord event = (GenericRecord) envelope.getMessage();

        String view = event.get("view").toString();
        long timestamp = (long) event.get("timestamp");
        // int userId = (int) event.get("userId");

        int x = (int) event.get("x");
        int xMax = (int) event.get("xMax");
        int y = (int) event.get("y");
        int yMax = (int) event.get("yMax");
/*
        System.out.println("");
        System.out.println("-----------------------------------");
        System.out.println("view: " + view + " length: " + view.length());
        System.out.println("timestamp " + timestamp);

        System.out.println("x " + x);
        System.out.println("xMax " + xMax);
        System.out.println("y " + y);
        System.out.println("yMax " + yMax);
*/
        // Temp hack: Ignore points not coming from "/"
        if (view.substring(1).isEmpty()) {
            System.out.println("--------UPDATE!------------");

            // Count points
            CountPoint countPoint = new CountPoint();
            countPoint.view = view;
            countPoint.timestamp = timestamp;

            // Avoid dividing through zero
            if (xMax == 0) {
                xMax = 1;
            }
            if (yMax == 0) {
                yMax = 1;
            }
            // Quantize
            countPoint.xx = (x * xNormMax) / xMax;
            countPoint.yy = (y * yNormMax) / yMax;

            // Count
            if (counts.containsKey(countPoint)) {
                int newCount = counts.get(countPoint) + 1;
                counts.put(countPoint, newCount);

                // Set local maximum
                if (pointRateMax < newCount) {
                    pointRateMax = newCount;
                }
                System.out.println("View:  " + countPoint.view + " with count=" + newCount + " for specific point: "
                        + Integer.toString(countPoint.xx) + "/" + Integer.toString(countPoint.yy));
            } else {
                System.out.println("creating new entry for view: " + countPoint.view + " with specific point: "
                        + countPoint.xx + "/" + countPoint.yy);
                counts.put(countPoint, 1);
            }

            // Pack and send
            if (pointRateMax == 0) {
                pointRateMax = 1;
            }
            int radius = counts.get(countPoint) * 100 / pointRateMax;
            int value = counts.get(countPoint) * 100 / pointRateMax;

            // Create db entry for new value,
            // Update otherwise
            HeatmapPoint heatmapPoint = new HeatmapPoint(radius, value, countPoint.xx, countPoint.yy, countPoint.view,
                    countPoint.hashCode());
            if (counts.get(countPoint) <= 1) {
                insertToMongoDb(heatmapPoint);
            } else {
                updateToMongoDb(heatmapPoint);
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

    private long calcTimeDiff(String time1, String time2) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = format.parse(time1);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            date2 = format.parse(time2);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return date2.getTime() - date1.getTime();
    }
}
package com.lambdanow.heatmap.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;

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
    private List<WriteModel<Document>> bulkUpdates = Arrays.<WriteModel<Document>> asList();

    private static int xNormMax = 800;
    private static int yNormMax = 600;
    int pointRateMax = 100;
    long firstTimestamp = 0;
    long lastAfterglowTimestamp = 0;
    private static long maxTimeDiffAfterglow = 60000; // milisec

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
        lastAfterglowTimestamp = firstTimestamp;
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {

        // System.out.println("window: --------------------------------------->");
        if (!counts.isEmpty()) {

            //
            // "Afterglow": ( as maxTimeDiffAfterglow )
            //
            long afterglowTimeDiff = System.currentTimeMillis() - lastAfterglowTimestamp;
            if (afterglowTimeDiff >= maxTimeDiffAfterglow) {
                this.afterglowUpdatePoints();
                lastAfterglowTimestamp = System.currentTimeMillis();
            }

            ///
            // Bulk Write (as window frequeny)
            //
            this.dropMongoCollection();
            this.insertPointsToMongoDb();
            // this.upsertBulkToMongoDb();
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
            } else {
                counts.put(countPoint, 1);
            }
        }
    }

    // http://stackoverflow.com/questions/31470702/bulk-upsert-with-mongodb-java-3-0-driver
    private void addToUpsertList(HeatmapPoint point) {

        Bson filter = new Document("hashCode", point.hashCode);
        Bson update = Updates.set("point", point);
        // Bson updateWeight = Updates.set("point.weight", point.value);
        // Bson updates = Updates.combine(updateRadius, updateWeight);

        UpdateOneModel<Document> uom = new UpdateOneModel<Document>(filter, // filter part
                update, // update part
                new UpdateOptions().upsert(true) // options like upsert
        );
        bulkUpdates.add(uom);
    }

    private void upsertBulkToMongoDb() {
        System.out.println("upsertBulkToMongoDb():");
        if (!counts.isEmpty()) {
            for (CountPoint key : counts.keySet()) {
                int newCount = counts.get(key);
                int radius = newCount * 100 / pointRateMax;
                int value = newCount * 100 / pointRateMax;
                HeatmapPoint hmp = new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode());
                this.addToUpsertList(hmp);
            }
            if (!bulkUpdates.isEmpty()) {
                BulkWriteResult bulkWriteResult = this.getCollection().bulkWrite(bulkUpdates);
                if (!bulkWriteResult.wasAcknowledged()) {
                    System.out.println("upsertBulkToMongoDb(): NOT acknoledged");
                } else {
                    System.out.println("upsertBulkToMongoDb(): acknoledged. Clear update cache");
                    bulkUpdates.clear();
                }
            } else {
                System.out.println("upsertBulkToMongoDb(): bulkUpdates EMPTY!");
            }
        }
    }

    private void insertPointsToMongoDb() {
        if (!counts.isEmpty()) {
            List<Document> docList = new ArrayList<Document>();
            for (CountPoint key : counts.keySet()) {
                int newCount = counts.get(key);
                int radius = newCount * 100 / pointRateMax;
                int value = newCount * 100 / pointRateMax;
                HeatmapPoint hmp = new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode());
                docList.add(getDocFromPoint(hmp));
            }
            System.out.println("insertPointsToMongoDb(): ");
            this.getCollection().insertMany(docList);
            System.out.println("insertPointsToMongoDb() point list inserted");
        } else {
            System.out.println("insertPointsToMongoDb(): List is empty");
        }
    }

    /*
     * deleteMany DeleteResult deleteMany(Bson filter) Removes all documents from the collection that match the given
     * query filter. If no documents match, the collection is not modified
     */
    private void afterglowUpdatePoints() {
        System.out.println("afterglowUpdatePoints()");
        for (CountPoint key : counts.keySet()) {
            long timeDiff = (long) key.timestamp - firstTimestamp;
            if (timeDiff >= maxTimeDiffAfterglow) {
                // scale counts, reduce the rate
                // int newCount = (int)(counts.get(key) * 0.75 );
                int newCount = counts.get(key) / 2;

                key.timestamp = System.currentTimeMillis();
                counts.put(key, newCount);

                if (pointRateMax == 0) {
                    pointRateMax = 1;
                }
                System.out.println("afterglowUpdatePoints(): Old Point: " + key.xx + " / " + key.yy + " in view "
                        + key.view + " updated! " + timeDiff);
            }
        }
    }

    private MongoCollection<Document> getCollection() {
        if (mongoCollection.isEmpty()) {
            System.out.println("ERROR: Mongocollection name is empty!");
        }
        return db.getCollection(this.mongoCollection);
    }

    private Document getDocFromPoint(HeatmapPoint point) {
        return new Document("point",
                new Document().append("view", point.view).append("x", point.xx).append("y", point.yy)
                        .append("radius", point.radius).append("weight", point.value)).append("hashCode",
                                point.hashCode);
    }

    private void dropMongoCollection() {
        System.out.println("dropMongoCollection()");
        this.getCollection().drop();
    }
    // // DEPRECATED
    // private void updateToMongoDb(HeatmapPoint point) {
    // // System.out.println("updateToMongoDb(): " + Integer.toString(point.value));
    //
    // Bson filter = new Document("hashCode", point.hashCode);
    // Bson updateRadius = Updates.set("point.radius", point.radius);
    // Bson updateWeight = Updates.set("point.weight", point.value);
    // Bson updates = Updates.combine(updateRadius, updateWeight);
    // UpdateResult res = this.getCollection().updateOne(filter, updates);
    // // UpdateOptions options = new UpdateOptions().upsert(true)
    //
    // if (res.wasAcknowledged()) {
    // System.out.println("Updating point... acknoledged");
    // } else {
    // System.out.println("Updating point... NOT acknoledged");
    // }
    // }
    //
    // // DEPRECATED
    // private void insertToMongoDb(HeatmapPoint point) {
    // System.out.println("insertToMongoDb(): ");
    // this.getCollection()
    // .insertOne(new Document("point",
    // new Document().append("view", point.view).append("x", point.xx).append("y", point.yy)
    // .append("radius", point.radius).append("weight", point.value)).append("hashCode",
    // point.hashCode));
    // System.out.println("point inserted");
    // }
}
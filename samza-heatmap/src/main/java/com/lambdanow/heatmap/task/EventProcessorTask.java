package com.lambdanow.heatmap.task;

import java.util.ArrayList;
import java.util.Collections;
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
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
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
            // this.insertPointsToMongoDb();
            this.upsertBulkToMongoDb();
            
            ///
            // clean up
            //
//            if (afterglowTimeDiff >= maxTimeDiffAfterglow) {
//                this.deleteZeroValues();
//                lastAfterglowTimestamp = System.currentTimeMillis();
//            }
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
                int newCount = (int)counts.get(countPoint) + 1;
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

    private void upsertBulkToMongoDb() {
        System.out.println("upsertBulkToMongoDb():");
        if (pointRateMax == 0) {
            pointRateMax = 1;
        }
        // Prepare upsert blob
        if (!counts.isEmpty()) {
            List<WriteModel<Document>> bulkUpdates = new ArrayList<WriteModel<Document>>();
            List<WriteModel<Document>> deletes = new ArrayList<WriteModel<Document>>();
            
            for (CountPoint key : counts.keySet()) {
                int newCount = (int)counts.get(key);
                // Upsert or delete
                if (newCount != 0) {
                    int radius = (newCount * 100) / pointRateMax;
                    int value = (newCount * 100) / pointRateMax;
                    HeatmapPoint hmp = new HeatmapPoint((int)radius, (int)value, (int)key.xx, (int)key.yy, key.view, (int)key.hashCode());
                    bulkUpdates.add(
                            new UpdateOneModel<Document>(
                                new Document("_id", key.hashCode()),  // filter part, hashCode
                                new Document("$set", getDocFromPoint(hmp)),           // update part
                                new UpdateOptions().upsert(true)
                            )
                        );
                } else {
                    deletes.add(
                            new DeleteManyModel<Document>(
                                new Document("_id", key.hashCode())
                            )
                        );
                }
            }
            
            //
            // Bulk Update
            bulkUpdates.addAll(deletes);
            if (!bulkUpdates.isEmpty()) {
                BulkWriteResult bulkWriteResult = this.getCollection().bulkWrite(bulkUpdates);
                if (!bulkWriteResult.wasAcknowledged()) {
                    System.out.println("upsertBulkToMongoDb(): NOT acknoledged");
                } else {
                    System.out.println("upsertBulkToMongoDb(): acknoledged. Clear update cache");
                    bulkUpdates.clear();
                }
            } 
        }
    }

    private void afterglowUpdatePoints() {
        System.out.println("afterglowUpdatePoints()");
        
//        Iterator<CountPoint> it = counts.keySet().iterator();
//        while (it.hasNext()) {
//            CountPoint cp = it.next();
//            int count = (int)counts.get(cp);
//            
//            System.out.println(cp.view + " = count: " + count);
//            long timeDiff = (long) cp.timestamp - firstTimestamp;
//            if (timeDiff >= maxTimeDiffAfterglow) {
//                // scale counts, reduce the rate
//                int newCount = (int)(count / 2); // count - (count / 4);
//
//                // Clean update hash map
//                // remove if count is zero
//                if ( newCount == 0 ) {
//                    System.out.println("afterglowUpdatePoints: remove");
//                    it.remove();
//                } else {
//                    cp.timestamp = System.currentTimeMillis();
//                    counts.put(cp, newCount);
//                }
//                
//                if (pointRateMax == 0) {
//                    pointRateMax = 1;
//                }
//                System.out.println("afterglowUpdatePoints(): Old Point: " + cp.xx + " / " + cp.yy + " in view "
//                        + cp.view + " updated! " + timeDiff);
//            }
//        }
        
        for (CountPoint cp : counts.keySet()) {
            long timeDiff = (long) cp.timestamp - firstTimestamp;
            if (timeDiff >= maxTimeDiffAfterglow) {
                // scale counts, reduce the rate
                int val = (int)counts.get(cp);
                int newCount = (int)(val / 2) ; // val - (val / 4);
                    cp.timestamp = System.currentTimeMillis();
                    counts.put(cp, newCount);
                    System.out.println("afterglowUpdatePoints(): Old Point: " + cp.xx + " / " + cp.yy + " in view "
                            + cp.view + " updated! " + timeDiff);
                if (pointRateMax == 0) {
                    pointRateMax = 1;
                }

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

    private void insertPointsToMongoDb() {
        this.dropMongoCollection();
        if (pointRateMax == 0) {
            pointRateMax = 1;
        }
        if (!counts.isEmpty()) {
            List<Document> docList = new ArrayList<Document>();
            for (CountPoint key : counts.keySet()) {
                int newCount = (int)counts.get(key);
                int radius = (newCount * 100) / pointRateMax;
                int value = (newCount * 100) / pointRateMax;
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
    private void dropMongoCollection() {
        System.out.println("dropMongoCollection()");
        this.getCollection().drop();
    }
    
    private void deleteZeroValues(){
        System.out.println("deleteZeroValues():");
        Bson filter = new Document("point.radius", 0);
        this.getCollection().deleteMany(filter);
        filter = new Document("point.weight", 0);
        this.getCollection().deleteMany(filter);
    }
}
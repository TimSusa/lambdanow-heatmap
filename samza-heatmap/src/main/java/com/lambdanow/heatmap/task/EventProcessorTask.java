package com.lambdanow.heatmap.task;

import java.util.ArrayList;
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
    private static final String AFTERGLOW = "task.afterglow.ms";
    private static final String X_NORM_MAX = "task.x.norm.max";
    private static final String Y_NORM_MAX = "task.y.norm.max";
    private static final String POINT_RATE_MAX = "task.point.rate.max";

    private String mongoCollection = "";
    private MongoClient mongoClient;
    private MongoDatabase db;

    private long maxTimeDiffAfterglow;
    private int xNormMax;
    private int yNormMax;
    private int pointRateMax;
    private int pointRateMaxStatic;
    private long firstTimestamp;
    private long lastAfterglowTimestamp;

    private Map<CountPoint, Integer> counts = new HashMap<CountPoint, Integer>();

    /**
     * init.
     * 
     */
    public void init(Config config, TaskContext context) {
        System.out.println("Init");
        mongoCollection = config.get(MONGO_COLLECTION);
        mongoClient = new MongoClient(config.get(MONGO_HOST), Integer.parseInt(config.get(MONGO_PORT)));
        db = mongoClient.getDatabase(config.get(MONGO_DB_NAME));
        maxTimeDiffAfterglow = Long.valueOf(config.get(AFTERGLOW)).longValue();
        xNormMax = Integer.parseInt(config.get(X_NORM_MAX));
        yNormMax = Integer.parseInt(config.get(Y_NORM_MAX));
        pointRateMax = Integer.parseInt(config.get(POINT_RATE_MAX)); // would be changed
        pointRateMaxStatic = Integer.parseInt(config.get(POINT_RATE_MAX)); // would never be changed
        firstTimestamp = System.currentTimeMillis();
        lastAfterglowTimestamp = firstTimestamp;
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {

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
            // Bulk Write (as window frequency)
            //
            this.upsertBulkToMongoDb();
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
        if (view.trim().equals("/")) {
            
            // printAllInput(timestamp, xx, xxMax, yy, yyMax);

            // Set count point and count
            checkAndCount(new CountPoint(view, quantize(xx, xNormMax, getOneIfZero(xxMax)),
                    quantize(yy, yNormMax, getOneIfZero(yyMax)), timestamp));

        }
    }

    private void checkAndCount(CountPoint countPoint) {
        if (counts.containsKey(countPoint)) {
            int newCount = (int) counts.get(countPoint) + 1;
            counts.put(countPoint, newCount);

            // Set local maximum
            if (pointRateMax < newCount) {
                pointRateMax = newCount;
            }

        } else {
            counts.put(countPoint, 1);
        }
    }

    private void upsertBulkToMongoDb() {
        if (pointRateMax == 0) {
            pointRateMax = 1;
        }
        // Prepare upsert blob
        if (!counts.isEmpty()) {
            List<WriteModel<Document>> bulkUpdates = new ArrayList<WriteModel<Document>>();
            List<WriteModel<Document>> bulkDeletes = new ArrayList<WriteModel<Document>>();

            for (CountPoint key : counts.keySet()) {
                int newCount = (int) counts.get(key);
                int radius = (newCount * pointRateMaxStatic) / pointRateMax;
                radius = clipToPointRateMaxStatic(radius);
                int value = (newCount * pointRateMaxStatic) / pointRateMax;
                value = clipToPointRateMaxStatic(value);
                
                // Upsert or delete
                if (radius != 0) {
                    HeatmapPoint hmp = new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode());
                    bulkUpdates.add(new UpdateOneModel<Document>(new Document("_id", key.hashCode()),
                            new Document("$set", getDocFromPoint(hmp)), new UpdateOptions().upsert(true)));
                } else {
                    bulkDeletes.add(new DeleteManyModel<Document>(new Document("_id", key.hashCode())));
                }
            }

            //
            // Bulk Update
            bulkUpdates.addAll(bulkDeletes);
            if (!bulkUpdates.isEmpty()) {
                BulkWriteResult bulkWriteResult = this.getCollection().bulkWrite(bulkUpdates);
                if (!bulkWriteResult.wasAcknowledged()) {
                    System.out.println("upsertBulkToMongoDb(): NOT acknoledged");
                } else {
                    System.out.println("upsertBulkToMongoDb(): acknoledged. Clear update cache");
                    bulkUpdates.clear();
                    bulkDeletes.clear();
                }
            }
        }
    }

    private void afterglowUpdatePoints() {
        System.out.println("afterglowUpdatePoints()");
        long timestampNow = System.currentTimeMillis();
        for (CountPoint cp : counts.keySet()) {
            long timeDiff = timestampNow - (long) cp.timestamp;
            if (timeDiff >= maxTimeDiffAfterglow) {
                // scale counts, reduce the rate
                int val = (int) counts.get(cp);
                int newCount = shrink(val, 3, 0);
                pointRateMax = shrink(val, 6, 1);
                cp.timestamp = timestampNow;
                counts.put(cp, newCount);
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

    private int shrink(int input, int scaleFac, int reduceTo) {
        int retVal = (input != 1) ? (input - (input / scaleFac)) : reduceTo;
        return (input == 2) ? 1 : retVal;
    }

    private int getOneIfZero(int input) {
        return (input == 0) ? 1 : input;
    }

    private int quantize(int input, int norm, int max) {
        return (input * norm) / max;
    }
    
    private int clipToPointRateMaxStatic(int input){
        return (input > pointRateMaxStatic) ? pointRateMaxStatic : input;
    }
    
    private void printAllInput(long timestamp, int xx, int xxMax, int yy, int yyMax){
         System.out.println("-----------------------------------");
         System.out.println("timestamp " + timestamp);
         System.out.println("x " + xx);
         System.out.println("xMax " + xxMax);
         System.out.println("y " + yy);
         System.out.println("yMax " + yyMax);
    }
}
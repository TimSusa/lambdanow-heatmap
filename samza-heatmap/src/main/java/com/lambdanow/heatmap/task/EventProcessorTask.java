package com.lambdanow.heatmap.task;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.jsonwebtoken.UnsupportedJwtException;

/**
 * Class HeatmapPoint.
 * 
 * @author timsusa
 *
 */
final class HeatmapPoint {

    public int radius;

    public int value;

    public int xx;

    public int yy;

    public String view;

    public int hashCode;

    public int userId;

    public HeatmapPoint(int radius, int value, int xx, int yy, String view, int hashCode, int userId) {
        this.radius = radius;
        this.value = value;
        this.xx = xx;
        this.yy = yy;
        this.view = view;
        this.hashCode = hashCode;
        this.userId = userId;
    }
}

/**
 * Class CountPoint.
 * 
 * @author timsusa
 *
 */
final class CountPoint {
    public String view;
    public int xx;
    public int yy;
    public long timestamp;
    public int userId;

    public CountPoint(String view, int xx, int yy, long timestamp, int userId) {
        this.view = view;
        this.xx = xx;
        this.yy = yy;
        this.timestamp = timestamp;
        this.userId = userId;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
        // if deriving: appendSuper(super.hashCode()).
                append(view).append(xx).append(yy).append(userId).toHashCode();
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
                .append(view, rhs.view).append(xx, rhs.xx).append(yy, rhs.yy).append(userId, rhs.userId).isEquals();
    }
}

/**
 * Class EventProcessorTask.
 * 
 * @author timsusa
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
    private int maxCount;
    private int pointRateMaxStatic;
    private long firstTimestamp;
    private long lastAfterglowTimestamp;
    private static final String SECRET_KEY = "d9f9u.e6a??.,./arefowi42"; // This is taken to create the token

    private Map<CountPoint, Integer> counts = new HashMap<CountPoint, Integer>();
    private Map<String, Integer> trackingTokenMap = new HashMap<String, Integer>();

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.samza.task.InitableTask#init(org.apache.samza.config.Config, org.apache.samza.task.TaskContext)
     */
    public void init(Config config, TaskContext context) {
        mongoCollection = config.get(MONGO_COLLECTION);
        mongoClient = new MongoClient(config.get(MONGO_HOST), Integer.parseInt(config.get(MONGO_PORT)));
        db = mongoClient.getDatabase(config.get(MONGO_DB_NAME));

        maxTimeDiffAfterglow = Long.valueOf(config.get(AFTERGLOW)).longValue();
        xNormMax = Integer.parseInt(config.get(X_NORM_MAX));
        yNormMax = Integer.parseInt(config.get(Y_NORM_MAX));
        maxCount = Integer.parseInt(config.get(POINT_RATE_MAX)); // would be changed
        pointRateMaxStatic = Integer.parseInt(config.get(POINT_RATE_MAX)); // would never be changed
        firstTimestamp = System.currentTimeMillis();
        lastAfterglowTimestamp = firstTimestamp;
        System.out.println("Init OK");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.samza.task.WindowableTask#window(org.apache.samza.task.MessageCollector,
     * org.apache.samza.task.TaskCoordinator)
     */
    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {

        if (!counts.isEmpty()) {

            // "Afterglow": ( as maxTimeDiffAfterglow )
            long afterglowTimeDiff = System.currentTimeMillis() - lastAfterglowTimestamp;
            if (afterglowTimeDiff >= maxTimeDiffAfterglow) {
                this.afterglowUpdatePoints();
                lastAfterglowTimestamp = System.currentTimeMillis();
            }

            // Bulk Write (as window frequency)
            this.upsertBulkToMongoDb();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.samza.task.StreamTask#process(org.apache.samza.system.IncomingMessageEnvelope,
     * org.apache.samza.task.MessageCollector, org.apache.samza.task.TaskCoordinator)
     */
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        GenericRecord event = (GenericRecord) envelope.getMessage();

        String view = event.get("view").toString();
        long timestamp = System.currentTimeMillis(); // (long) event.get("timestamp");
        int xx = (int) event.get("x");
        int xxMax = (int) event.get("xMax");
        int yy = (int) event.get("y");
        int yyMax = (int) event.get("yMax");

        String userIdString = event.get("userId").toString();
        int userId = getUserIdByToken(userIdString);

        printAllInput(timestamp, xx, xxMax, yy, yyMax, userId);
        
        // Temp hack: Ignore points not coming from "/"
        if (view.trim().equals("/")) {



            // Set quantized countPoint and count
            checkAndCount(new CountPoint(view, quantize(xx, xNormMax, getOneIfZero(xxMax)),
                    quantize(yy, yNormMax, getOneIfZero(yyMax)), timestamp, userId));

        }
    }

    /**
     * Checks for existing countPoint in the map and counts up its rate.
     * 
     * @param countPoint
     */
    private void checkAndCount(CountPoint countPoint) {
        if (counts.containsKey(countPoint)) {
            int newCount = (int) counts.get(countPoint) + 1;
            counts.put(countPoint, newCount);

            // Set local maximum
            if (maxCount < newCount) {
                maxCount = newCount;
            }

        } else {
            counts.put(countPoint, 1);
        }
    }

    /**
     * Upsert points to mongoDb.
     */
    private void upsertBulkToMongoDb() {

        // Prepare upsert
        if (!counts.isEmpty()) {
            List<WriteModel<Document>> bulkUpdates = new ArrayList<WriteModel<Document>>();
            List<WriteModel<Document>> bulkDeletes = new ArrayList<WriteModel<Document>>();

            for (CountPoint key : counts.keySet()) {
                int newCount = (int) counts.get(key);
                int radius = (newCount * pointRateMaxStatic) / getOneIfZero(maxCount);
                radius = clip(radius, 100);
                int value = clip(radius * 10, 100);

                // Upsert or delete
                if (radius != 0) {
                    HeatmapPoint hmp = new HeatmapPoint(radius, value, key.xx, key.yy, key.view, key.hashCode(),
                            key.userId);
                    bulkUpdates.add(new UpdateOneModel<Document>(new Document("_id", key.hashCode()),
                            new Document("$set", getDocFromPoint(hmp)), new UpdateOptions().upsert(true)));
                } else {
                    bulkDeletes.add(new DeleteManyModel<Document>(new Document("_id", key.hashCode())));
                }
            }

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

    /**
     * Afterglow points due tue windowing function.
     */
    private void afterglowUpdatePoints() {
        System.out.println("afterglowUpdatePoints()");
        long timestampNow = System.currentTimeMillis();
        for (CountPoint cp : counts.keySet()) {
            long timeDiff = timestampNow - (long) cp.timestamp;
            int val = (int) counts.get(cp);
            if (timeDiff >= maxTimeDiffAfterglow) {
                // scale counts, reduce the rate
                int newCount = shrink(val, 3, 0);
                cp.timestamp = timestampNow;
                counts.put(cp, newCount);
            }
            if (timeDiff >= (3 * maxTimeDiffAfterglow)) {
                maxCount = shrink(val, 8, 1);
            }
        }

    }

    /**
     * @return Mongo collection object (via string from config file).
     */
    private MongoCollection<Document> getCollection() {
        if (mongoCollection.isEmpty()) {
            System.out.println("ERROR: Mongocollection name is empty!");
        }
        return db.getCollection(this.mongoCollection);
    }

    /**
     * @param point
     * @return Document object consisting of point data.
     */
    private Document getDocFromPoint(HeatmapPoint point) {
        return new Document("point",
                new Document().append("view", point.view).append("x", point.xx).append("y", point.yy)
                        .append("radius", point.radius).append("weight", point.value))
                                .append("hashCode", point.hashCode).append("userId", point.userId);
    }

    /**
     * @param input
     * @param scaleFac
     * @param reduceTo
     * @return shrinked value as integer.
     */
    private int shrink(int input, int scaleFac, int reduceTo) {
        int retVal = (input != 1) ? (input - (input / scaleFac)) : reduceTo;
        return (input == 2) ? 1 : retVal;
    }

    /**
     * @param input
     * @return input as integer or 1, if input is 0.
     */
    private int getOneIfZero(int input) {
        return (input == 0) ? 1 : input;
    }

    /**
     * @param input
     * @param norm
     * @param max
     * @return quantized input value as integer.
     */
    private int quantize(int input, int norm, int max) {
        return (input * norm) / max;
    }

    /**
     * @param input
     * @return clipped input value as integer.
     */
    private int clip(int input, int clipVal) {
        return (input > clipVal) ? clipVal : input;
    }

    /**
     * Method to validate and read out the JWT. Decrypts and extracts userinfo.
     * 
     * @param jwt
     *            Token as String
     * @return userId
     */
    public Integer getUserIdByToken(String jwt) {
        
        System.out.println("getUserIdByToken(): " + jwt);

        Integer retVal = -1000;

        if (trackingTokenMap.containsKey(jwt)) {
            retVal = trackingTokenMap.get(jwt);
        } else {
            try {

                Claims claims = Jwts.parser().setSigningKey(DatatypeConverter.parseBase64Binary(SECRET_KEY))
                        .parseClaimsJws(jwt).getBody();

                // OK, we can trust this JWT
                System.out.println("getUserIdByToken(): OK, we can trust this JWT");

                System.out.println("getUserIdByToken(): ID = " + claims.getId());
                System.out.println("getUserIdByToken(): Subject = " + claims.getSubject());
                System.out.println("getUserIdByToken(): Issuer = " + claims.getIssuer());
                System.out.println("getUserIdByToken(): Expiration = " + claims.getExpiration());
                
                
                if (StringUtils.isNumeric(claims.getId())) {
                    retVal = Integer.parseInt(claims.getId());
                }
                
                // this.trackingTokenMap.put(jwt, retVal);
                
            } catch (SignatureException e) {
                // don't trust the JWT!
                System.out.println("getUserIdByToken(): claimsJws JWS signature validation fail");
            } catch (UnsupportedJwtException e) {
                System.out.println("getUserIdByToken(): claimsJws argument does not represent an Claims JWS");
            } catch (MalformedJwtException e) {
                System.out.println("getUserIdByToken(): claimsJws string is not a valid JWS");
            } catch (ExpiredJwtException e) {
                System.out.println("getUserIdByToken(): Claims has an expiration time before the time this method is invoked");
            } catch (IllegalArgumentException e) {
                System.out.println("getUserIdByToken(): IllegalArgumentException");
            }
        }
        
        return retVal;
    }
    

    /**
     * Prit out all input values.
     * 
     * @param timestamp
     * @param xx
     * @param xxMax
     * @param yy
     * @param yyMax
     */
    private void printAllInput(long timestamp, int xx, int xxMax, int yy, int yyMax, int userId) {
        System.out.println("-----------------------------------");
        System.out.println("timestamp " + timestamp);
        System.out.println("x " + xx);
        System.out.println("xMax " + xxMax);
        System.out.println("y " + yy);
        System.out.println("yMax " + yyMax);
        System.out.println("userId " + userId);
    }
}
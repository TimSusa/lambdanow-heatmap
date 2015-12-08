package com.lambdanow.sparkanalytics;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.lambdanow.sparkanalytics.serializer.AvroDecoder;
import com.typesafe.config.Config;
import org.apache.avro.generic.GenericRecord;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka.*;

import com.typesafe.config.ConfigFactory;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public final class AnalyticsCollector {
    static Logger log = Logger.getLogger(AnalyticsCollector.class.getName());

    public static void main(String[] args) throws Exception {
        /**
         * Read configs for both kafka & influxDB
         */
        Config config = ConfigFactory.load();

        String influxHost = config.getString("influx.host");
        String influxUser = config.getString("influx.user");
        String influxPass = config.getString("influx.pass");
        String influxDbName = config.getString("influx.db");

        InfluxDB influxClient = InfluxDBFactory.connect(influxHost, influxUser, influxPass);
        influxClient.createDatabase(influxDbName);

        // Kafka
        String[] topics = config.getString("kafka.topics").split(" ");
        Map<String, Integer> topicsMap = new HashMap<>();
        for(String topic : topics) {
            topicsMap.put(topic, 1); // 1 worker per topic
        }

        SparkConf conf = new SparkConf().setAppName("SparkAnalytics");
        // Create a StreamingContext with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));


        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("group.id", "spark_events");
        kafkaParams.put("zookeeper.connect", config.getString("zookeeper.connect"));

        AvroDecoder decoder = new AvroDecoder();

        JavaPairDStream<String, byte[]> input = KafkaUtils.createStream(jssc, String.class, byte[].class,
                kafka.serializer.StringDecoder.class, kafka.serializer.DefaultDecoder.class, kafkaParams, topicsMap, StorageLevel.MEMORY_ONLY());

        //input.print();
        saveRssIntoInflux(input, decoder, influxClient, influxDbName);

        // start our streaming context and wait for it to "finish"
        jssc.start();
        // Wait until batch is processed
        jssc.awaitTermination();
        // Stop the streaming context
        jssc.stop();
    }

    public static void saveRssIntoInflux(JavaPairDStream<String, byte[]> input, final AvroDecoder decoder, InfluxDB influxClient, String databaseName) {
        input.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, byte[]> rdd) throws Exception {
                try {
                    // Get the list of Generic records
                    JavaRDD<byte[]> valuesRdd = rdd.values();
                    List<byte[]> values = valuesRdd.collect();

                    // Construct InfluxDB batch
                    BatchPoints batchPoints = BatchPoints.database(databaseName)
                            .retentionPolicy("default")
                            .consistency(InfluxDB.ConsistencyLevel.ALL)
                            .tag("async", "true")
                            .build();

                    boolean sendBatch = false;
                    for (byte[] data : values) {
                        GenericRecord record = decoder.fromBytes(data);

                        String actor = record.get("actor") + "i";
                        String action = record.get("action").toString();
                        String object = record.get("object").toString();

                        /**
                         * Register all fields as tags and fields at the same time.
                         * Not a very good design given that tags are indexed and fields are not.
                         * But this is for demonstration purposes and should suffice
                         */

                        Point p = Point.measurement("event")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .field("actor", actor)
                                .field("action", action)
                                .field("object", object)
                                .build();

                        batchPoints.point(p);
                        sendBatch = true;
                    }

                    if(sendBatch) {
                        log.info(String.format("Writing %d points", batchPoints.getPoints().size()));
                        influxClient.write(batchPoints);
                    }
                }
                catch(Exception e) {
                    log.error("Exception", e);
                }

                return null;
            }
        });
    }

}
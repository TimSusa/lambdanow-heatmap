package com.lambdanow.analytics.task;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;


import java.util.concurrent.TimeUnit;

public class EventProcessorTask  implements StreamTask, InitableTask {
    static final private String CFGPREFIX = "task.influx.";

    protected InfluxDB influxClient;
    protected String influxHost;
    protected String influxUser;
    protected String influxPass;
    protected String influxDbName;


    @Override
    public void init(Config config, TaskContext context) {
        influxHost = config.get(CFGPREFIX + "host");
        influxUser = config.get(CFGPREFIX + "user");
        influxPass = config.get(CFGPREFIX + "pass");
        influxDbName = config.get(CFGPREFIX + "db");

        influxClient = InfluxDBFactory.connect(influxHost, influxUser, influxPass);
        influxClient.createDatabase(influxDbName);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        GenericRecord event = (GenericRecord) envelope.getMessage();

        String actor = event.get("actor") + "i";
        String action = event.get("action").toString();
        String object = event.get("object").toString();

        BatchPoints batchPoints = BatchPoints.database(influxDbName)
                .retentionPolicy("default")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .tag("async", "true")
                .build();

        /**
         * Register all fields as tags and fields at the same time.
         * Not a very good design given that tags are indexed and fields are not.
         * But this is for demonstration purposes and should suffice
         */

        Point p = Point.measurement("event")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("actor", actor)
                .tag("action", action)
                .tag("object", object)
                .field("actor", actor)
                .field("action", action)
                .field("object", object)
                .build();

        batchPoints.point(p);

        influxClient.write(batchPoints);
    }
}
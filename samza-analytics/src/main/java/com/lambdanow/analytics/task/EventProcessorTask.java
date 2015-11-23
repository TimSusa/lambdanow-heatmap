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

        BatchPoints batchPoints = BatchPoints.database(influxDbName)
                .retentionPolicy("default")
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .tag("async", "true")
                .build();

        Point p = Point.measurement("event")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("userId", event.get("userId").toString() + "i")
                .tag("eventCategory", event.get("eventCategory").toString())
                .tag("eventAction", event.get("eventAction").toString())
                .field("eventLabel", event.get("eventLabel").toString() + "i")
                .field("eventValue", event.get("eventValue"))
                .build();

        batchPoints.point(p);

        influxClient.write(batchPoints);
    }
}
package com.lambdanow.analytics.serializer;

import com.lambdanow.avro.schema.*;
import com.lambdanow.avro.serde.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;

import java.util.Map;
import java.util.Properties;

public class AvroRecordFactory implements SerdeFactory<GenericRecord> {
    @Override
    public Serde<GenericRecord> getSerde(String name, Config config) {
        Config schemaRegistryConfig = config.subset("schema.registry");

        return new AvroSerde(getSchemaRegistry(schemaRegistryConfig));
    }

    private SchemaRegistry getSchemaRegistry(Config config) {
        SchemaRegistry registry = new RemoteMemorySchemaRegistry();

        Properties props = new Properties();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            props.put(entry.getKey().replace(".", ""), entry.getValue());
        }

        System.out.println(props);

        registry.init(props);

        return registry;
    }

    private static class AvroSerde extends FingerprintSerdeGeneric implements Serde<GenericRecord> {
        public AvroSerde(SchemaRegistry schemaRegistry) {
            super(schemaRegistry);
        }
    }
}

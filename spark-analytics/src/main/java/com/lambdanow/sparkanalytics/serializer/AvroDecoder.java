package com.lambdanow.sparkanalytics.serializer;

import com.lambdanow.avro.schema.RemoteMemorySchemaRegistry;
import com.lambdanow.avro.serde.FingerprintSerdeGeneric;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

public class AvroDecoder
{
    private FingerprintSerdeGeneric fingerprintSerde;

    static Logger log = Logger.getLogger(AvroDecoder.class.getName());

    public AvroDecoder() {
        log.info("Initialize");
        Config config = ConfigFactory.load();

        String schemaRegistryUrl = config.getString("schema.registry.url");
        String schemaRegistryToken = config.getString("schema.registry.token");

        // Construct the value decoder
        RemoteMemorySchemaRegistry schemaRegistry = getRemoteRegistry(schemaRegistryUrl, schemaRegistryToken);
        fingerprintSerde = getFingerprintSerde(schemaRegistry);
    }

    public GenericRecord fromBytes(byte[] bytes) {
        return fingerprintSerde.fromBytes(bytes);
    }

    protected RemoteMemorySchemaRegistry getRemoteRegistry(String url, String token) {
        log.info("Initialize Schema Registry with URL: " + url);
        Properties props = new Properties();
        props.setProperty("url", url);
        props.setProperty("token", token);
        props.setProperty("remote", "true");

        RemoteMemorySchemaRegistry registry = new RemoteMemorySchemaRegistry();
        registry.init(props);
        return registry;
    }

    protected FingerprintSerdeGeneric getFingerprintSerde(RemoteMemorySchemaRegistry registry) {
        return new FingerprintSerdeGeneric(registry);
    }
}

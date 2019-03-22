package io.infinivision.flink.connectors.postgres;

import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.infinivision.flink.connectors.postgres.PostgresValidator.CONNECTOR_VERSION_VALUE_95;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.*;

public class PostgresTableFactory implements
        StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Row>,
        BatchTableSourceFactory<Row>,
        BatchTableSinkFactory<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableFactory.class);

    public static final String DRIVERNAME = "org.postgresql.Driver";

    String postgresVersion() {
        return CONNECTOR_VERSION_VALUE_95;
    }

    String DriverName() {
        return  "org.postgresql.Driver";
    }

    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        preCheck(properties);
        TableProperties prop = new TableProperties();
        prop.putProperties(properties);
        RichTableSchema schema = prop.readSchemaFromProperties(Thread.currentThread().getContextClassLoader());
        String[] columnNames = schema.getColumnNames();
        InternalType[] columnTypes = schema.getColumnTypes();
        boolean[] nullables = schema.getNullables();

        Preconditions.checkArgument(columnNames.length > 0 && columnTypes.length > 0 && nullables.length > 0,
                "column numbers can not be 0");

        Preconditions.checkArgument(columnNames.length == columnTypes.length,
                "columnNames length must be equal to columnTypes length");

        Preconditions.checkArgument(columnNames.length == nullables.length,
                "columnNames length must be equal to nullable length");


        PostgresTableSource.Builder builder = PostgresTableSource.builder()
                .fields(columnNames, columnTypes, nullables);

        Set<String> primaryKeys = new HashSet<>();
        Set<Set<String>> uniqueKeys = new HashSet<>();
        Set<Set<String>> normalIndexes = new HashSet<>();
        if (!schema.getPrimaryKeys().isEmpty()) {
//            uniqueKeys.add(new HashSet<>(schema.getPrimaryKeys()));
            primaryKeys = new HashSet<>(schema.getPrimaryKeys());
        }
        for (List<String> uniqueKey : schema.getUniqueKeys()) {
            uniqueKeys.add(new HashSet<>(uniqueKey));
        }
        for (RichTableSchema.Index index : schema.getIndexes()) {
            if (index.unique) {
                uniqueKeys.add(new HashSet<>(index.keyList));
            } else {
                normalIndexes.add(new HashSet<>(index.keyList));
            }
        }
        if (!primaryKeys.isEmpty()) {
            builder.setPrimaryKeys(primaryKeys);
        }
        if (!uniqueKeys.isEmpty()) {
            builder.setUniqueKeys(uniqueKeys);
        }
        if (!normalIndexes.isEmpty()) {
            builder.setNormalIndexes(normalIndexes);
        }

        return builder.build();

    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        return null;
    }

    @Override
    public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
        throw new UnsupportedOperationException("Postgres table can not be convert to Batch Table Source currently.");
    }

    @Override
    public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
        throw new UnsupportedOperationException("Postgres table can not be convert to Batch Table Sink currently.");
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "POSTGRES");
        context.put(CONNECTOR_VERSION, postgresVersion()); // version
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        return PostgresOptions.SUPPORTED_KEYS;
    }

    private void preCheck(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(properties.getOrDefault(PostgresOptions.USER_NAME.key(), ""), false, 1);
        descriptorProperties.validateString(properties.getOrDefault(PostgresOptions.PASSWORD.key(), ""), false, 1);
        descriptorProperties.validateString(properties.getOrDefault(PostgresOptions.DB_URL.key(), ""), false, 1);
        descriptorProperties.validateString(properties.getOrDefault(PostgresOptions.TABLE_NAME.key(), ""), false, 1);
    }
}

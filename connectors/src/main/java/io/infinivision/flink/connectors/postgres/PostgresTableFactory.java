package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.JDBCUpsertTableSink;
import io.infinivision.flink.connectors.utils.JDBCTableOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.*;
import java.util.stream.Collectors;

import static io.infinivision.flink.connectors.postgres.PostgresValidator.CONNECTOR_VERSION_VALUE_94;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.*;

public class PostgresTableFactory implements
        StreamTableSourceFactory<BaseRow>,
        StreamTableSinkFactory<BaseRow>,
        BatchTableSourceFactory<BaseRow>,
        BatchTableSinkFactory<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableFactory.class);

    public static final String DRIVERNAME = "org.postgresql.Driver";

    public static String postgresVersion() {
        return CONNECTOR_VERSION_VALUE_94;
    }

    @Override
    public StreamTableSource<BaseRow> createStreamTableSource(Map<String, String> properties) {
        TableProperties prop = new TableProperties();
        prop.putProperties(properties);

        new PostgresValidator().validateTableOptions(properties);

        RichTableSchema schema = prop.readSchemaFromProperties(
                Thread.currentThread().getContextClassLoader()
        );
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
            uniqueKeys.add(new HashSet<>(schema.getPrimaryKeys()));
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

        builder.setTableProperties(prop);

        return builder.build();

    }

    private TableSink createJDBCUpsertStreamTableSink(Map<String, String> properties) {
        TableProperties prop = new TableProperties();
        prop.putProperties(properties);

        new PostgresValidator().validateTableOptions(properties);

        RichTableSchema schema = prop.readSchemaFromProperties(
                Thread.currentThread().getContextClassLoader()
        );
        String[] columnNames = schema.getColumnNames();
        InternalType[] columnTypes = schema.getColumnTypes();
        boolean[] nullables = schema.getNullables();

        Preconditions.checkArgument(columnNames.length > 0 && columnTypes.length > 0 && nullables.length > 0,
                "column numbers can not be 0");

        Preconditions.checkArgument(columnNames.length == columnTypes.length,
                "columnNames length must be equal to columnTypes length");

        Preconditions.checkArgument(columnNames.length == nullables.length,
                "columnNames length must be equal to nullable length");


        JDBCUpsertTableSink.Builder builder = JDBCUpsertTableSink.builder()
                .userName(prop.getString(JDBCOptions.USER_NAME))
                .password(prop.getString(JDBCOptions.PASSWORD))
                .dbURL(prop.getString(JDBCOptions.DB_URL))
                .driverName(DRIVERNAME)
                .driverVersion(postgresVersion())
                .tableName(prop.getString(JDBCOptions.TABLE_NAME));

        Set<String> primaryKeys = new HashSet<>();
        Set<Set<String>> uniqueKeys = new HashSet<>();
        if (!schema.getPrimaryKeys().isEmpty()) {
            uniqueKeys.add(new HashSet<>(schema.getPrimaryKeys()));
            primaryKeys = new HashSet<>(schema.getPrimaryKeys());
        }
        for (List<String> uniqueKey : schema.getUniqueKeys()) {
            uniqueKeys.add(new HashSet<>(uniqueKey));
        }
        for (RichTableSchema.Index index : schema.getIndexes()) {
            if (index.unique) {
                uniqueKeys.add(new HashSet<>(index.keyList));
            }
        }

        if (primaryKeys.isEmpty() && uniqueKeys.isEmpty()) {
            throw new IllegalArgumentException("JDBCUpsertTableSink should at least contain one primary key or one unique index");
        } else if (!primaryKeys.isEmpty()) {
            if (primaryKeys.size() == columnNames.length) {
                throw new IllegalArgumentException("JDBCUpsertTableSink primary key fields size should less than total column size");
            }
            builder.uniqueKeys(Option.apply(primaryKeys));
        } else {
            if (uniqueKeys.size() != 1) {
                throw new IllegalArgumentException("JDBCUpsertTableSink should contain only one unique index");
            } else if (uniqueKeys.size() == columnNames.length) {
                throw new IllegalArgumentException("JDBCUpsertTableSink unique key size should less than total column size");
            }

            builder.uniqueKeys(Option.apply(uniqueKeys.iterator().next()));
        }

        builder.schema(schema);

        return builder.build()
                .configure(schema.getColumnNames(), schema.getColumnTypes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public StreamTableSink<BaseRow> createStreamTableSink(Map<String, String> properties) {
        return (StreamTableSink<BaseRow>)createJDBCUpsertStreamTableSink(properties);
    }

    @Override
    public BatchTableSource<BaseRow> createBatchTableSource(Map<String, String> properties) {
        throw new UnsupportedOperationException("Postgres table can not be convert to Batch Table Source currently.");
    }

    @Override
    public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> properties) {
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
        List<String> properties = new ArrayList<>(JDBCOptions.SUPPORTED_KEYS);
        properties.addAll(JDBCTableOptions.SUPPORTED_KEYS);
        return properties;
    }

}

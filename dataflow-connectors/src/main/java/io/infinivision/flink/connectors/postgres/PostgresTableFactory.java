package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.utils.JDBCTableOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataTypes;
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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.*;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.*;

public class PostgresTableFactory implements
        StreamTableSourceFactory<BaseRow>,
        StreamTableSinkFactory<BaseRow>,
        BatchTableSourceFactory<BaseRow>,
        BatchTableSinkFactory<BaseRow> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresTableFactory.class);

    public static final String DRIVERNAME = "org.postgresql.Driver";

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

    private TableSink createJDBCStreamTableSink(Map<String, String> properties) {
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


        // validate bitmap field
        String bitmapField = prop.getString(PostgresTableOptions.BITMAP_FIELD);
        boolean hasBitmapField = false;
        if (bitmapField != null) {
            int length = columnNames.length;
            int index = 0;
            for (; index < length; index++) {
                if (bitmapField.equals(columnNames[index])) {
                    break;
                }
            }

            if (index == length) {
                throw new IllegalArgumentException(String.format("bitmapField: %s was not in the column list", bitmapField));
            }

            // check the column type. the bitmap field type must be VARBINARY
            if (!columnTypes[index].equals(DataTypes.BYTE_ARRAY)) {
                throw new IllegalArgumentException("bitmapField type must be VARBINARY");
            }

            hasBitmapField = true;
        }

        PostgresTableSink.Builder builder = PostgresTableSink.builder();
        builder.userName(prop.getString(JDBCOptions.USER_NAME))
                .password(prop.getString(JDBCOptions.PASSWORD))
                .dbURL(prop.getString(JDBCOptions.DB_URL))
                .driverName(DRIVERNAME)
                .driverVersion(prop.getString(JDBCTableOptions.VERSION))
                .tableName(prop.getString(JDBCOptions.TABLE_NAME))
                .updateMode(prop.getString(JDBCTableOptions.UPDATE_MODE));

        if (hasBitmapField) {
            builder.bitmapField(Option.apply(bitmapField));
        }

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

        if (!primaryKeys.isEmpty()) {
            builder.primaryKeys(Option.apply(primaryKeys));
        }

        if (!uniqueKeys.isEmpty()) {
            builder.uniqueKeys(Option.apply(uniqueKeys));
        }

        builder.schema(Option.apply(schema));

        builder.setParameterTypes(schema.getColumnTypes());

        return builder.build()
                .configure(schema.getColumnNames(), schema.getColumnTypes());
    }

    @SuppressWarnings("unchecked")
    @Override
    public StreamTableSink<BaseRow> createStreamTableSink(Map<String, String> properties) {
        return (StreamTableSink<BaseRow>) createJDBCStreamTableSink(properties);
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
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>(JDBCOptions.SUPPORTED_KEYS);
        properties.addAll(PostgresTableOptions.SUPPORTED_KEYS);
        return properties;
    }

}

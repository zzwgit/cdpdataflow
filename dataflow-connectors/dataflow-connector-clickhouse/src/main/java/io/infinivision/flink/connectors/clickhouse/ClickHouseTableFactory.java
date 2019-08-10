package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.utils.CommonTableOptions;
import io.infinivision.flink.connectors.utils.JDBCTableOptions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.api.RichTableSchema;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.factories.BatchCompatibleTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchCompatibleStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.util.TableProperties;
import org.apache.flink.util.Preconditions;
import scala.Option;

import java.util.*;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;

//BatchTableSinkFactory<BaseRow>, BatchCompatibleTableSinkFactory<BaseRow>
public class ClickHouseTableFactory implements
        StreamTableSinkFactory<BaseRow>,
        BatchCompatibleTableSinkFactory<BaseRow>,
        StreamTableSourceFactory<BaseRow>,
        BatchTableSourceFactory<BaseRow> {

    public static final String DRIVERNAME = "ru.yandex.clickhouse.ClickHouseDriver";

    @Override
    public BatchTableSource<BaseRow> createBatchTableSource(Map<String, String> properties) {
        return createSource(properties);
    }

    @Override
    public StreamTableSource<BaseRow> createStreamTableSource(Map<String, String> properties) {
        return createSource(properties);
    }

//    @Override
//    public BatchTableSink<BaseRow> createBatchTableSink(Map<String, String> properties) {
//        return (BatchTableSink<BaseRow>) createJDBCStreamTableSink(properties);
//    }

    @Override
    public BatchCompatibleStreamTableSink<BaseRow> createBatchCompatibleTableSink(Map<String, String> properties) {
        return (BatchCompatibleStreamTableSink<BaseRow>) createJDBCStreamTableSink(properties);
    }

    @Override
    public StreamTableSink<BaseRow> createStreamTableSink(Map<String, String> properties) {
        return (StreamTableSink<BaseRow>) createJDBCStreamTableSink(properties);
    }

    private ClickHouseTableSource createSource(Map<String, String> properties) {
        TableProperties prop = new TableProperties();
        prop.putProperties(properties);

        ClickHouseValidator.validateTableOptions(properties);

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


        ClickHouseTableSourceBuilder builder = ClickHouseTableSourceBuilder.builder()
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

        ClickHouseValidator.validateTableOptions(properties);

        RichTableSchema schema = prop.readSchemaFromProperties(
                Thread.currentThread().getContextClassLoader()
        );

        String[] columnNames = schema.getColumnNames();
        InternalType[] columnTypes = schema.getColumnTypes();
        boolean[] nullables = schema.getNullables();

        Preconditions.checkArgument(columnNames.length > 0 && columnTypes.length > 0 && nullables.length > 0,
                "column numbers can not be 0!");

        Preconditions.checkArgument(columnNames.length == columnTypes.length,
                "columnNames length must be equal to columnTypes length!");

        Preconditions.checkArgument(columnNames.length == nullables.length,
                "columnNames length must be equal to nullable length!");

        // validate array field
        String field = prop.getString(ClickHouseTableOptions.ARRAY_FIELD);
        String[] arrayField = StringUtils.isNotBlank(field) ? StringUtils.split(field, ",") : null;

        if (ArrayUtils.isNotEmpty(arrayField)) {
            for (String item : arrayField) {
                int length = columnNames.length;
                int index = 0;
                for (; index < length; index++) {
                    if (item.equals(columnNames[index])) {
                        break;
                    }
                }

                if (index == length) {
                    throw new IllegalArgumentException(String.format("arrayField: %s was not in the column list", item));
                }

                // check the column type. the array field type must be VARBINARY
                if (!columnTypes[index].equals(DataTypes.BYTE_ARRAY)) {
                    throw new IllegalArgumentException("arrayField type must be VARBINARY");
                }
            }
        }

        //ClickHouseTableSinkBuilder.
        ClickHouseTableSinkBuilder builder = ClickHouseTableSinkBuilder.builder();
        builder.driverName(DRIVERNAME)
                .dbURL(prop.getString(JDBCOptions.DB_URL))
                .driverVersion(prop.getString(JDBCTableOptions.VERSION))
                .batchSize(Integer.parseInt(prop.getString(CommonTableOptions.BATCH_SIZE)))
                .userName(prop.getString(JDBCOptions.USER_NAME))
                .password(prop.getString(JDBCOptions.PASSWORD))
                .tableName(prop.getString(JDBCOptions.TABLE_NAME))
                .updateMode(prop.getString(JDBCTableOptions.UPDATE_MODE))
                .servers(prop.getString(JDBCTableOptions.SERVERS));

        if (ArrayUtils.isNotEmpty(arrayField)) {
            builder.setArrayFields(arrayField);
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

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR_TYPE, "CLICKHOUSE");
        context.put(CONNECTOR_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>(JDBCOptions.SUPPORTED_KEYS);
        properties.addAll(JDBCTableOptions.SUPPORTED_KEYS);
        properties.addAll(ClickHouseTableOptions.SUPPORTED_KEYS);
        return properties;
    }

}

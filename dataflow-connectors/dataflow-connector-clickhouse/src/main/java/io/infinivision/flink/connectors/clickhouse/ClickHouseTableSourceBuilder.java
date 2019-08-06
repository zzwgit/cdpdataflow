package io.infinivision.flink.connectors.clickhouse;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.TypeConverters;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.util.TableProperties;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseTableSourceBuilder {

    private ClickHouseTableSource clickHouseTableSource;
    private LinkedHashMap<String, Tuple2<InternalType, Boolean>> schema = new LinkedHashMap<>();
    private Set<String> primaryKeys = new HashSet<>();
    private Set<Set<String>> uniqueKeys = new HashSet<>();
    private Set<Set<String>> normalIndexes = new HashSet<>();
    TableProperties tableProperties;

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(Set<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public Set<Set<String>> getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(Set<Set<String>> uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public Set<Set<String>> getNormalIndexes() {
        return normalIndexes;
    }

    public void setNormalIndexes(Set<Set<String>> normalIndexes) {
        this.normalIndexes = normalIndexes;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(TableProperties tableProperties) {
        this.tableProperties = tableProperties;
    }

    public ClickHouseTableSourceBuilder tableProperties(TableProperties tableProperties) {
        this.tableProperties = tableProperties;
        return this;
    }

    public ClickHouseTableSourceBuilder field(String columnName,
                                             TypeInformation columnType) {
        if (schema.containsKey(columnName)) {
            throw new IllegalArgumentException("duplicate column: " + columnName);
        }

        InternalType internalType = TypeConverters.createInternalTypeFromTypeInfo(columnType);
        boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(internalType);
        schema.put(columnName, new Tuple2<>(internalType, nullable));
        return this;
    }

    public ClickHouseTableSourceBuilder field(String columnName,
                                             InternalType columnType) {
        if (schema.containsKey(columnName)) {
            throw new IllegalArgumentException("duplicate column: " + columnName);
        }

        boolean nullable = !FlinkTypeFactory.isTimeIndicatorType(columnType);
        schema.put(columnName, new Tuple2<>(columnType, nullable));
        return this;
    }

    public ClickHouseTableSourceBuilder field(String columnName,
                                             InternalType columnType,
                                             boolean nullable) {
        if (schema.containsKey(columnName)) {
            throw new IllegalArgumentException("duplicate column: " + columnName);
        }

        schema.put(columnName, new Tuple2<>(columnType, nullable));
        return this;
    }

    public ClickHouseTableSourceBuilder fields(String[] columnNames,
                                              InternalType[] columnTypes,
                                              boolean[] nullables){
        for (int index=0; index<columnNames.length; index++) {
            field(columnNames[index], columnTypes[index], nullables[index]);
        }
        return this;
    }

    public static ClickHouseTableSourceBuilder builder(){
        return new ClickHouseTableSourceBuilder();
    }

    public ClickHouseTableSource build() {
        if (schema.isEmpty()) {
            throw new IllegalArgumentException("Postgres Table source fields can't be empty");
        }

        return new ClickHouseTableSource(
                tableProperties,
                schema.keySet().toArray(new String[0]),
                schema.values()
                        .stream()
                        .map(x -> x.f0)
                        .collect(Collectors.toList())
                        .toArray(new InternalType[0]),
                schema.values()
                        .stream()
                        .map(x -> x.f1)
                        .collect(Collectors.toList())
                        .toArray(new Boolean[0]),
                primaryKeys.toArray(new String[0]),
                uniqueKeys
                        .stream()
                        .map(u -> u.toArray(new String[0]))  // mapping each List to an array
                        .collect(Collectors.toList())               // collecting as a List<String[]>
                        .toArray(new String[uniqueKeys.size()][]),

                normalIndexes
                        .stream()
                        .map(u -> u.toArray(new String[0]))  // mapping each List to an array
                        .collect(Collectors.toList())               // collecting as a List<String[]>
                        .toArray(new String[normalIndexes.size()][])
        );
    }
}

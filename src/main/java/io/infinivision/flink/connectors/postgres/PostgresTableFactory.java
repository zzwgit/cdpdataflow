package io.infinivision.flink.connectors.postgres;

import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class PostgresTableFactory implements
        StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Row>,
        BatchTableSourceFactory<Row>,
        BatchTableSinkFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        return null;
    }

    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        return null;
    }

    @Override
    public BatchTableSource<Row> createBatchTableSource(Map<String, String> properties) {
        return null;
    }

    @Override
    public BatchTableSink<Row> createBatchTableSink(Map<String, String> properties) {
        return null;
    }

    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }
}

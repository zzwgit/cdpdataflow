package io.infinivision.flink.table;

import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.factories.BatchTableSourceFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.types.Row;

public class PostgresTableFactory implements
        StreamTableSourceFactory<Row>,
        StreamTableSinkFactory<Row>,
        BatchTableSourceFactory<Row>,
        BatchTableSinkFactory<Row> {

}

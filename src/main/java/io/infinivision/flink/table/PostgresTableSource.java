package io.infinivision.flink.table;

import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class PostgresTableSource implements
        StreamTableSource<Row>,
        BatchTableSource<Row>,
        LookupableTableSource<Row> {

}

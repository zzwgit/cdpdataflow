package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat;
import io.infinivision.flink.connectors.jdbc.JDBCTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

public class ClickHouseTableSink extends JDBCTableSink {

    private final JDBCBaseOutputFormat outputFormat;

    public ClickHouseTableSink(JDBCBaseOutputFormat outputFormat) {
        super(outputFormat);
        this.outputFormat = outputFormat;
    }

    @Override
    public void setKeyFields(String[] keys) {

    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {

    }

    @Override
    public TableSinkBase<Tuple2<Boolean, Row>> copy() {
        return new ClickHouseTableSink(outputFormat);
    }
}

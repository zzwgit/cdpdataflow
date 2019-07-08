package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat;
import io.infinivision.flink.connectors.jdbc.JDBCTableSink;
import io.infinivision.flink.connectors.jdbc.JDBCTableSinkBuilder;

public class ClickHouseTableSinkBuilder extends JDBCTableSinkBuilder {

    public static ClickHouseTableSinkBuilder builder() {
        return new ClickHouseTableSinkBuilder();
    }

    @Override
    public JDBCTableSink build() {

        JDBCBaseOutputFormat outputFormat = new ClickHouseAppendOutputFormat(
                this.userName(),
                this.password(),
                this.driverName(),
                this.driverVersion(),
                this.dbURL(),
                this.tableName(),
                this.schema().get().getColumnNames(),
                this.parameterTypes(),
                this.batchSize()
        );

        return new ClickHouseTableSink(outputFormat);
    }
}

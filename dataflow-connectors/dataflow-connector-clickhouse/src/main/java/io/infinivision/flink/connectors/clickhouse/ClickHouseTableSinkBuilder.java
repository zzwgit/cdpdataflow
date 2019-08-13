package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.jdbc.JDBCBaseOutputFormat;
import io.infinivision.flink.connectors.jdbc.JDBCTableSink;
import io.infinivision.flink.connectors.jdbc.JDBCTableSinkBuilder;
import org.apache.commons.lang3.StringUtils;

public class ClickHouseTableSinkBuilder extends JDBCTableSinkBuilder {

    private String[] arrayFields;

    public static ClickHouseTableSinkBuilder builder() {
        return new ClickHouseTableSinkBuilder();
    }

    public String[] getArrayFields() {
        return arrayFields;
    }

    public void setArrayFields(String[] arrayFields) {
        this.arrayFields = arrayFields;
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
                this.batchSize(),
                this.arrayFields,
                StringUtils.isBlank(this.servers()) ? null : this.servers().split(";"),
                this.asyncFlush()
        );

        return new ClickHouseTableSink(outputFormat);
    }
}

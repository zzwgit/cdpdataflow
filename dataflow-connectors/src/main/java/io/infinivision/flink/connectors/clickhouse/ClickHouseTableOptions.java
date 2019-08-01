package io.infinivision.flink.connectors.clickhouse;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class ClickHouseTableOptions {
    // array field
    public static final ConfigOption<String> ARRAY_FIELD = key("arrayField".toLowerCase())
            .noDefaultValue();

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            ARRAY_FIELD.key()
    );

}

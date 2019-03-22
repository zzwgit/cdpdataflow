package io.infinivision.flink.connectors.postgres;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class PostgresOptions {
    public static final ConfigOption<String> USER_NAME = key("username".toLowerCase())
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = key("password".toLowerCase())
            .noDefaultValue();

    public static final ConfigOption<String> DRIVER_NAME = key("drivername".toLowerCase())
            .noDefaultValue();

    public static final ConfigOption<String> TABLE_NAME = key("tablename".toLowerCase())
            .noDefaultValue();

    public static final ConfigOption<String> DB_URL = key("dburl".toLowerCase())
            .noDefaultValue();

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            USER_NAME.key(),
            PASSWORD.key(),
            DRIVER_NAME.key(),
            TABLE_NAME.key(),
            DB_URL.key());
}

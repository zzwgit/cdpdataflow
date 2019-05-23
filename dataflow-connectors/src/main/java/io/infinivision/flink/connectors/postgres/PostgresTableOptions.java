package io.infinivision.flink.connectors.postgres;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class PostgresTableOptions {
    // bitmap field
    public static final ConfigOption<String> BITMAP_FIELD = key("bitmapField".toLowerCase())
            .noDefaultValue();

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            BITMAP_FIELD.key()
    );

}

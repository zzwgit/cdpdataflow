package io.infinivision.flink.connectors.http;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class HttpTableOptions {

    public static final ConfigOption<String> REQUEST_URL = key("request.url".toLowerCase())
            .noDefaultValue();

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            REQUEST_URL.key()
    );

}

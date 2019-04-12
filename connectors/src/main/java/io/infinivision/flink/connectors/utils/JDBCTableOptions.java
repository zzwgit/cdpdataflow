package io.infinivision.flink.connectors.utils;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class JDBCTableOptions {

    public enum JOIN_MODE {
        SYNC,
        ASYNC;

        public static boolean isValid(String type) {
            for (JOIN_MODE ct : JOIN_MODE.values()) {
                if (ct.name().equalsIgnoreCase(type)) {
                    return true;
                }
            }
            return false;
        }
    }

    public enum CacheType {
        NONE, LRU, ALL;

        public static boolean isValid(String type) {
            for (CacheType ct : CacheType.values()) {
                if (ct.name().equalsIgnoreCase(type)) {
                    return true;
                }
            }
            return false;
        }
    }

    // database version
    public static final ConfigOption<String> VERSION = key("version".toLowerCase())
            .defaultValue("9.5");

    // database update mode
    public static final ConfigOption<String> UPDATE_MODE = key("updateMode".toLowerCase())
            .defaultValue("upsert");

    // cache Type - NONE, LRU, ALL
    public static final ConfigOption<String> CACHE = key("cache".toLowerCase())
            .defaultValue("none");

    // cache TTL for LRU
    public static final ConfigOption<String>  CACHE_TTL = key("cacheTTLms".toLowerCase())
            .defaultValue("3600000");

    // join mode. sync / async
    public static final ConfigOption<String>  MODE = key("mode".toLowerCase())
            .defaultValue("async");

    // async join buffer capacity
    public static final ConfigOption<String>  BUFFER_CAPACITY = key("buffercapacity".toLowerCase())
            .defaultValue("100");

    // timeout for the async collectors
    public static final ConfigOption<String>  TIMEOUT = key("asynctimeout".toLowerCase())
            .defaultValue("10000");

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            VERSION.key(),
            CACHE.key(),
            CACHE_TTL.key(),
            MODE.key(),
            BUFFER_CAPACITY.key(),
            TIMEOUT.key()
    );


}

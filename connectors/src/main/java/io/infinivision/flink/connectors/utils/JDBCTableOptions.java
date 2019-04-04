package io.infinivision.flink.connectors.utils;

import java.util.Arrays;
import java.util.List;

public class JDBCTableOptions {

    public static enum JOIN_MODE {
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
    // cache Type - NONE, LRU, ALL
    public static final String CACHE = "cache";

    // cache TTL for LRU
    public static final String CACHE_TTL = "cachettlms";

    // join mode. sync / async
    public static final String MODE = "mode";

    // async join buffer capacity
    public static final String BUFFER_CAPACITY = "buffercapacity";

    // timeout for the async collectors
    public static final String TIMEOUT = "asynctimeout";

    public static final List<String> SUPPORTED_KEYS = Arrays.asList(
            CACHE,
            CACHE_TTL,
            MODE,
            BUFFER_CAPACITY,
            TIMEOUT
    );


}

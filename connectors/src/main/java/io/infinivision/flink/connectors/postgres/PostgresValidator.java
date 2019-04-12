package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.utils.JDBCTableOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class PostgresValidator extends ConnectorDescriptorValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    public static final String CONNECTOR_VERSION_VALUE_94 = "9.4";
    public static final String CONNECTOR_VERSION_VALUE_95 = "9.5";

    public void validateTableOptions(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(JDBCOptions.USER_NAME.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.PASSWORD.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.DB_URL.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.TABLE_NAME.key(), false, 1);

        // validate version
        List<String> supportVersions = Arrays.asList(CONNECTOR_VERSION_VALUE_94, CONNECTOR_VERSION_VALUE_95);
        descriptorProperties.validateEnumValues(JDBCTableOptions.VERSION.key(), true, supportVersions);

        // validate cache
        if (properties.containsKey(JDBCTableOptions.CACHE.key())) {
            String cache = properties.get(JDBCTableOptions.CACHE.key());
            boolean isValid = JDBCTableOptions.CacheType.isValid(cache);
            if (!isValid) {
                throw new ValidationException("cache type only support NONE/LRU/ALL but the given is: "
                        + cache);
            }

            // validate LRU TTL if any
            JDBCTableOptions.CacheType cacheType = JDBCTableOptions.CacheType.valueOf(cache.toUpperCase());
            if (cacheType == JDBCTableOptions.CacheType.LRU) {
                descriptorProperties.validateInt(JDBCTableOptions.CACHE_TTL.key(), false);
            }
        }
    }

    public void validateTableLookupOptions(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);

        // validate look up mode
        String mode = properties.getOrDefault(JDBCTableOptions.MODE.key(), JDBCTableOptions.JOIN_MODE.ASYNC.name());
        if (!JDBCTableOptions.JOIN_MODE.isValid(mode)) {
            throw new ValidationException("look up mode only support sync and async but the given is: "
                    + mode);
        }

        // validate async collector timeout
        descriptorProperties.validateInt(JDBCTableOptions.TIMEOUT.key(), true);

        // validate buffer capactiy
        descriptorProperties.validateInt(JDBCTableOptions.BUFFER_CAPACITY.key(), true);
    }

}

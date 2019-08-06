package io.infinivision.flink.connectors.utils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public abstract class CommonTableOptionsValidator extends ConnectorDescriptorValidator {

    public static void validateCacheOption(DescriptorProperties descriptorProperties) {
        // validate cache

        if (descriptorProperties.containsKey(CommonTableOptions.CACHE.key())) {
            String cache = descriptorProperties.getString(CommonTableOptions.CACHE.key());
            boolean isValid = CommonTableOptions.CacheType.isValid(cache);
            if (!isValid) {
                throw new ValidationException("cache type only support NONE/LRU/ALL but the given is: "
                        + cache);
            }

            // validate LRU TTL if any
            CommonTableOptions.CacheType cacheType = CommonTableOptions.CacheType.valueOf(cache.toUpperCase());
            if (cacheType == CommonTableOptions.CacheType.LRU) {
                descriptorProperties.validateInt(CommonTableOptions.CACHE_TTL.key(), false);
            }
        }
    }

    public static void validateTableLookupOptions(DescriptorProperties descriptorProperties) {

        // validate look up mode
        if (descriptorProperties.containsKey(CommonTableOptions.MODE.key())) {
            String mode = descriptorProperties.getString(CommonTableOptions.MODE.key());
            if (!CommonTableOptions.JOIN_MODE.isValid(mode)) {
                throw new ValidationException("look up mode only support sync and async but the given is: "
                        + mode);
            }

            // validate async collector timeout
            descriptorProperties.validateInt(CommonTableOptions.TIMEOUT.key(), true);

            // validate buffer capactiy
            descriptorProperties.validateInt(CommonTableOptions.BUFFER_CAPACITY.key(), true);
        }
    }
}

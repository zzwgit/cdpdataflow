package io.infinivision.flink.connectors.clickhouse;

import io.infinivision.flink.connectors.utils.CommonTableOptions;
import io.infinivision.flink.connectors.utils.JDBCTableOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ClickHouseValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseValidator.class);

    public static final String APPEND_MODE = "append";

    public static final List<String> UPDATE_MODES = Arrays.asList(APPEND_MODE);

    public static void validateTableOptions(Map<String, String> properties) {

        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);

        descriptorProperties.validateString(JDBCOptions.DB_URL.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.TABLE_NAME.key(), false, 1);

        if (descriptorProperties.containsKey(JDBCOptions.USER_NAME.key())) {
            descriptorProperties.validateString(JDBCOptions.USER_NAME.key(), false, 1);
            descriptorProperties.validateString(JDBCOptions.PASSWORD.key(), false, 1);
        }

        // validate update mode
        descriptorProperties.validateEnumValues(JDBCTableOptions.UPDATE_MODE.key(), true, UPDATE_MODES);
    }
}

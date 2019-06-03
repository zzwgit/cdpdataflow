package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.utils.CommonTableOptions;
import io.infinivision.flink.connectors.utils.CommonTableOptionsValidator;
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

public class PostgresValidator extends CommonTableOptionsValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    public static final String CONNECTOR_VERSION_VALUE_94 = "9.4";
    public static final String CONNECTOR_VERSION_VALUE_95 = "9.5";

    public static final String APPEND_MODE = "append";
    public static final String UPSERT_MODE = "upsert";

    public static final List<String> CONNECT_VERSIONS = Arrays.asList(CONNECTOR_VERSION_VALUE_94, CONNECTOR_VERSION_VALUE_95);
    public static final List<String> UPDATE_MODES = Arrays.asList(APPEND_MODE, UPSERT_MODE);

    public static void validateTableOptions(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(JDBCOptions.USER_NAME.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.PASSWORD.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.DB_URL.key(), false, 1);
        descriptorProperties.validateString(JDBCOptions.TABLE_NAME.key(), false, 1);

        // validate version
        descriptorProperties.validateEnumValues(JDBCTableOptions.VERSION.key(), true, CONNECT_VERSIONS);

        // validate update mode
        descriptorProperties.validateEnumValues(JDBCTableOptions.UPDATE_MODE.key(), true, UPDATE_MODES);

        // validate cache
        validateCacheOption(descriptorProperties);

        // validate Lookup Mode(async / sync)
        validateTableLookupOptions(descriptorProperties);
    }


}

package io.infinivision.flink.connectors.postgres;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class PostgresValidator extends ConnectorDescriptorValidator {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresValidator.class);

    public static final String CONNECTOR_TYPE_VALUE = "POSTGRES";
    public static final String CONNECTOR_VERSION_VALUE_94 = "9.4";
    public static final String CONNECTOR_VERSION_VALUE_95 = "9.5";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE, false);
        validateVersion(properties);
    }

    private void validateVersion(DescriptorProperties properties) {
        final List<String> versions = Arrays.asList(
                CONNECTOR_VERSION_VALUE_94,
                CONNECTOR_VERSION_VALUE_95);
        properties.validateEnumValues(CONNECTOR_VERSION, false, versions);
    }
}

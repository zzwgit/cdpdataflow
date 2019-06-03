package io.infinivision.flink.connectors.hbase

import java.util

import io.infinivision.flink.connectors.hbase.HBase121Validator.AuthMode.AuthMode
import io.infinivision.flink.connectors.utils.CommonTableOptionsValidator
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.connectors.hbase.table.HBaseValidator.CONNECTOR_HBASE_TABLE_NAME
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.util.StringUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import io.infinivision.flink.connectors.hbase.HBase121Validator._
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._

class HBase121Validator extends Logging{

  def validateTableOptions(properties: util.Map[String, String]): Unit ={
    LOG.info(s"HBase Properties: $properties")
    val descriptorProperties = new DescriptorProperties()
    descriptorProperties.putProperties(properties)
    // validate table Name
    descriptorProperties.validateString(CONNECTOR_HBASE_TABLE_NAME, false, 1)

    // validate zookeeper quorum
    val zkQuorum = properties.get(HConstants.ZOOKEEPER_QUORUM)
    if (StringUtils.isNullOrWhitespaceOnly(zkQuorum)) {
      // get from HBase configuration (need to include hbase-site.xml in the classpath)
      val conf = HBaseConfiguration.create()
      val zkQ = conf.get(HConstants.ZOOKEEPER_QUORUM)
      if (StringUtils.isNullOrWhitespaceOnly(zkQ)) {
        throw new RuntimeException("HBase zookeeper quorum should not be empty. please ensure the hbase-site.xml is the current classpath")
      }
    }

    // validate HBase version
    validateVersion(descriptorProperties)

    //for HBase put / get / delete batch operation
    validateBatchSize(descriptorProperties)

    // validate cache(LRU/ALL/NONE)
//    CommonTableOptionsValidator.validateCacheOption(descriptorProperties)

    // validate Lookup options
//    CommonTableOptionsValidator.validateTableLookupOptions(descriptorProperties)

    // validate auth
//    validateAuthLoginOption(descriptorProperties)
  }

  def validateVersion(properties: DescriptorProperties): Unit = {
    val versions = List(CONNECTOR_VERSION_VALUE_121)
    properties.validateEnumValues(CONNECTOR_HBASE_VERSION, false, versions.asJava)
  }

  /**
    * verify batch size for table sink
    * @param properties table properties
    */
  def validateBatchSize(properties: DescriptorProperties): Unit = {
    properties.validateInt(CONNECTOR_HBASE_BATCH_SIZE.key(), true, 1)
  }

  /**
    * validate SIMPLE/KERBEROS login for HBase Async Join(Lookup)
    * @param properties table properties
    */
  def validateAuthLoginOption(properties: DescriptorProperties): Unit = {

    // if auth enabled
    if(properties.containsKey(ASYNC_SECURITY_AUTH_ENABLE.key()) &&
      properties.getBoolean(ASYNC_SECURITY_AUTH_ENABLE.key())) {

      // kerberos validation
      if (!properties.containsKey(ASYNC_SECURITY_AUTHENTICATION.key())) {
        throw new IllegalArgumentException("security authentication(simple/kerberos) is needed")
      }

      if (properties.getString(ASYNC_SECURITY_AUTHENTICATION.key())
        .equalsIgnoreCase("kerberos")) {

        // validate kerberos principal
        properties.validateString(ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL.key(), false, 1)

        properties.validateString(ASYNC_SASL_CLIENTCONFIG.key(), false, 1)
        properties.validateString(ASYNC_AUTH_LOGIN_CONFIG.key(), false, 1)
      } else {
        throw new IllegalArgumentException("only simple/kerberos mode is supported for security authentication")
      }
    }
  }
}

object HBase121Validator {
  val CONNECTOR_VERSION_VALUE_121 = "1.2.1"
  val CONNECTOR_HBASE_VERSION = "version"
  val HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal"

  object AuthMode extends Enumeration {
    type AuthMode = Value
    val SIMPLE, KERBEROS = Value
  }

  // batch size for HBase PUT/DELETE/GET
  val CONNECTOR_HBASE_BATCH_SIZE: ConfigOption[Int] = ConfigOptions.key("batchSize".toLowerCase)
    .defaultValue(10000)

  // async Auth Login
  val ASYNC_SECURITY_AUTH_ENABLE: ConfigOption[Boolean] = ConfigOptions.key("hbase.security.auth.enable")
    .defaultValue(true)

    val ASYNC_SECURITY_AUTHENTICATION: ConfigOption[AuthMode] = ConfigOptions.key("hbase.security.authentication")
      .defaultValue(AuthMode.KERBEROS)

  val ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL: ConfigOption[String] = ConfigOptions.key("hbase.kerberos.regionserver.principal")
    .noDefaultValue()
  val ASYNC_RPC_PROTECTION: ConfigOption[String] = ConfigOptions.key("hbase.rpc.protection")
    .defaultValue("authentication")

  val ASYNC_SASL_CLIENTCONFIG: ConfigOption[String] = ConfigOptions.key("hbase.sasl.clientconfig")
    .defaultValue("HBaseClient")

  val ASYNC_AUTH_LOGIN_CONFIG: ConfigOption[String] = ConfigOptions.key("java.security.auth.login.config")
    .noDefaultValue()

}

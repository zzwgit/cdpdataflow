package io.infinivision.flink.connectors.hbase

import java.util
import java.lang.{Integer => JInteger}
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.util.Collections

import com.stumbleupon.async.Callback
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer
import org.apache.flink.core
import org.apache.flink.core.fs.local.LocalFileSystem
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem
import org.apache.flink.runtime.security.{DynamicConfiguration, KerberosUtils}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.functions.{AsyncTableFunction, FunctionContext}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.util.{Logging, TableProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.hbase.async._

import scala.collection.JavaConverters._

class HBaseAsyncLookupFunction(
  tableProperties: TableProperties,
  tableSchema: RichTableSchema,
  hbaseTableName: String,
  hbaseSchema: HBaseTableSchemaV2,
  rowKeySourceIndex: Int,
  qualifierSourceIndexes: util.List[JInteger],
  hbaseConfiguration: Configuration)
  extends AsyncTableFunction[BaseRow]
  with Logging{

  private val qualifierList: util.List[JTuple3[Array[Byte], Array[Byte], TypeInformation[_]]] = hbaseSchema.getFamilySchema.getFlatByteQualifiers
  private val charset: String = hbaseSchema.getFamilySchema.getStringCharset
  private val inputFieldSerializers: util.List[HBaseBytesSerializer] = new util.ArrayList[HBaseBytesSerializer]()
  private val totalQualifiers: Int = hbaseSchema.getFamilySchema.getTotalQualifiers

//  private val serializedConfig: Array[Byte] = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration)
//  private val rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(hbaseSchema.getRowKeyType)

  @transient private var hClient: HBaseClient = _

  for (index <- 0 to totalQualifiers) {
    if (index == rowKeySourceIndex) {
      inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType, charset))
    } else {
      val typeInfo = if (index < rowKeySourceIndex) qualifierList.get(index)  else qualifierList.get(index-1)
      inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset))
    }

  }

  def setupSecurityConfig(): Unit = {
    val priorConfig = javax.security.auth.login.Configuration.getConfiguration
    val currentConfig = new DynamicConfiguration(priorConfig)
    val loginContextName = tableProperties.getString(HBase121Validator.ASYNC_SASL_CLIENTCONFIG)
    var keyTabPath = tableProperties.getString(HBase121Validator.KEYTAB_PATH)
    val principal = tableProperties.getString(HBase121Validator.PRINCIPAL)
    // check the keytabPath
    LOG.info(s"AsyncHBaseLookup keytab: $keyTabPath, principal: $principal")
    val fs = FileSystem.get(URI.create(keyTabPath))
    if (!fs.exists(new Path(keyTabPath))) {
      throw new RuntimeException(s"AsyncHBaseLookup keyTabPath: $keyTabPath not exists")
    }
    if (fs.isDistributedFS) {
      val inputStream = fs.open(new Path(keyTabPath))
      val targetPath = Files.createTempFile("Flink-AsyncHBase-", ".keytab")
      Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING)
      val localPath = targetPath.toUri.getPath
      LOG.info(s"HBaseAsyncLookupFunction KeyTab HDFS Remote $keyTabPath. LocalPath: $localPath")
      keyTabPath = localPath
      targetPath.toFile.deleteOnExit()
      inputStream.close()
    }

    currentConfig.addAppConfigurationEntry(loginContextName, KerberosUtils.keytabEntry(keyTabPath, principal))
    javax.security.auth.login.Configuration.setConfiguration(currentConfig)
  }

  override def open(context: FunctionContext): Unit = {
    LOG.info("start open HBaseAsyncLookupFunction...")
    super.open(context)

    LOG.info(s"HBaseAsyncLookupFunction TableProperties: $tableProperties")

    val isSecurityEnabled = tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE).toBoolean
    if (isSecurityEnabled) {
      setupSecurityConfig()
    }

    val asyncConfig = new Config()

    asyncConfig.overrideConfig(HConstants.ZOOKEEPER_QUORUM,
      tableProperties.getString(HConstants.ZOOKEEPER_QUORUM, "localhost"))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SECURITY_AUTHENTICATION.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTHENTICATION))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL.key(),
      tableProperties.getString(HBase121Validator.ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_RPC_PROTECTION.key(),
      tableProperties.getString(HBase121Validator.ASYNC_RPC_PROTECTION))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SASL_CLIENTCONFIG.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SASL_CLIENTCONFIG))

    LOG.info("=====Dump HBase Async Configuration=====")
    LOG.info(asyncConfig.dumpConfiguration())

    hClient = new HBaseClient(asyncConfig)
    LOG.info("end open HBaseAsyncLookupFunction...")
  }

  override def close(): Unit = {
    LOG.info("start close HBaseAsyncLookupFunction...")

    super.close()

    if (null != hClient) {
      hClient.shutdown().join(5000)
    }
    LOG.info("end close HBaseAsyncLookupFunction...")

  }


  def parseResult(rowKey: AnyRef, cells: util.ArrayList[KeyValue]): GenericRow = {
    val reusedRow = new GenericRow(totalQualifiers+1)
    reusedRow.update(rowKeySourceIndex, rowKey)
    cells.asScala.foreach( cell => {
      val family = cell.family()
      val qualifier = cell.qualifier()
      val qInfo = qualifierList.asScala.filter { qf =>
        util.Arrays.equals(family, qf.f0) && util.Arrays.equals(qualifier, qf.f1)
      }

      if (qInfo.isEmpty) {
        LOG.info(s"can not find family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
        throw new RuntimeException(s"can not find family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
      }

      if (qInfo.size != 1) {
        LOG.info(s"duplicated family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
        throw new RuntimeException(s"duplicated family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
      }
      val qualifierSrcIdx = qualifierSourceIndexes.get(qualifierList.indexOf(qInfo.head))
      reusedRow.update(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(cell.value()))

    })

    reusedRow
  }

  def eval(resultFuture: ResultFuture[BaseRow], rowKey: Object): Unit = {
    val rk = inputFieldSerializers.get(rowKeySourceIndex).toHBaseBytes(rowKey)
    val getRequest: GetRequest = new GetRequest(hbaseTableName, rk)
    val defered = hClient.get(getRequest)

    defered.addCallback[Unit]( new Callback[Unit, util.ArrayList[KeyValue]] {
      override def call(cells: util.ArrayList[KeyValue]): Unit = {
        if (cells.size() == 0) {
          resultFuture.complete(Collections.emptyList())
        } else {
          val row = parseResult(rowKey, cells)
          resultFuture.complete(Collections.singletonList(row))
        }
      }
    })
  }
}

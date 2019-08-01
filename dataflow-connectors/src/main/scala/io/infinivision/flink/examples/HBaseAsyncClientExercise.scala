package io.infinivision.flink.examples

import java.util

import com.stumbleupon.async.Callback
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.runtime.security.{DynamicConfiguration, KerberosUtils}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.security.UserGroupInformation
import org.hbase.async._

import scala.collection.JavaConverters._

object HBaseAsyncClientExercise {

  def setupSecurityConfig(): Unit = {
    val priorConfig = javax.security.auth.login.Configuration.getConfiguration
    val currentConfig = new DynamicConfiguration(priorConfig)
    val loginContextName = "HBaseClient"
    //val keyTab = "D:\\test-files\\hongtaozhang.keytab"
    val keyTab = "D:\\test-files\\hongtaozhang.keytab"
    val principal = "hongtaozhang@MCD.COM.CN"
    currentConfig.addAppConfigurationEntry(loginContextName, KerberosUtils.keytabEntry(keyTab, principal))
    javax.security.auth.login.Configuration.setConfiguration(currentConfig)
  }

  //java "-Djava.security.auth.login.config=file:/home/flink/jass.conf"
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val zkQ = conf.get(HConstants.ZOOKEEPER_QUORUM)
    println(s"zookeeper Quorum: $zkQ")
    setupSecurityConfig()

    //    System.setProperty("java.security.auth.login.config", "/home/flink/hbase_jaas.conf")
    val asyncConfig = new Config()
    asyncConfig.overrideConfig("hbase.zookeeper.quorum", "shoyi05plpebdp019.mcd.com.cn,shoyi04plpebdp015.mcd.com.cn")
    asyncConfig.overrideConfig("hbase.security.auth.enable", "true")
    asyncConfig.overrideConfig("hbase.security.authentication", "kerberos")
    asyncConfig.overrideConfig("hbase.kerberos.regionserver.principal", "hbase/_HOST@MCD.COM.CN")
    asyncConfig.overrideConfig("hbase.rpc.protection", "authentication")
    asyncConfig.overrideConfig("hbase.sasl.clientconfig", "HBaseClient")
    //    asyncConfig.overrideConfig("java.security.auth.login.config", "/home/flink/jass.conf")
    println("====Configuration====")
    println(asyncConfig.dumpConfiguration())
    val hClient = new HBaseClient(asyncConfig)

    val tableName = "infinivision:id_mapping_mid"
    val rowKey = "mid1"
    val getRequest = new GetRequest(tableName, Bytes.UTF8(rowKey))
    val defered = hClient.get(getRequest)
    defered.addCallback[Unit](new Callback[Unit, util.ArrayList[KeyValue]] {
      override def call(cells: util.ArrayList[KeyValue]): Unit = {
        println("====CallBack====")
        println(s"response type: ${cells.getClass.getSimpleName}")
        println(s"cells size: ${cells.size()}")
        cells.asScala.foreach { cell =>
          println("====cell====")
          println(s"rowKey: ${Bytes.getInt(cell.key())}")
          println(s"family: ${Bytes.pretty(cell.family())}")
          println(s"qualifier: ${Bytes.pretty(cell.qualifier())}")
          println(s"value: ${Bytes.getInt(cell.value())}")

        }
      }
    })


    //    println("====HBase Scan====")
    //    val scanner = hClient.newScanner(tableName)
    //    val rows = scanner.nextRows(1000).join(10000)
    //    var count = 0
    //    rows.asScala.foreach { row =>
    //      println("====new Row====")
    //      row.asScala.foreach { cell =>
    //        println(s"rowKey: ${Bytes.getInt(cell.key())}")
    //        println(s"family: ${Bytes.pretty(cell.family())}")
    //        println(s"qualifier: ${Bytes.pretty(cell.qualifier())}")
    //        println(s"value: ${Bytes.getInt(cell.value())}")
    //      }
    //      count += 1
    //    }

    Thread.sleep(10000)
    hClient.shutdown()
  }
}

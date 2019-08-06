package io.infinivision.flink.connectors.hbase

import org.hbase.async.{Config, HBaseClient}

object HbaseClientInstance {

  var hbaseClient: HBaseClient = _
  val lock = new Object

  def getHbaseClient(asyncConfig: Config): HBaseClient = {
    lock.synchronized {
      if (hbaseClient == null) {
        hbaseClient = new HBaseClient(asyncConfig)
      }
      hbaseClient
    }
  }

}

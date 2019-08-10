package io.infinivision.flink.connectors.hbase

import org.hbase.async.{Config, HBaseClient}
import org.slf4j.LoggerFactory

object HbaseClientHolder {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  private var hbaseClient: HBaseClient = _
  private val lock = new Object

  def get(asyncConfig: Config): HBaseClient = {
    lock.synchronized {
      if (hbaseClient == null) {
        hbaseClient = new HBaseClient(asyncConfig)
      }
      hbaseClient
    }
  }
}

package io.infinivision.flink.examples

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConverters._

object HBaseClientExercise {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val tableName = "infinivision:ad_feature"
    val rowKey = 2118

    val hConnection = ConnectionFactory.createConnection(conf)
    val table = hConnection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

    // test get
    val result = table.get(new Get(Bytes.toBytes(rowKey)))
    result.listCells().asScala.foreach { cell =>
      println("====new row====")
      val rowKey = Bytes.toInt(CellUtil.cloneRow(cell))
      val family = Bytes.toString(CellUtil.cloneFamily(cell))
      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
      val value = Bytes.toInt(CellUtil.cloneValue(cell))
      println(rowKey)
      println(family)
      println(qualifier)
      println(value)
    }

    if (null != table) {
      table.close()
    }

    if (null != hConnection) {
      hConnection.close()
    }
  }
}

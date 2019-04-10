package io.infinivision.flink.connectors

import java.lang.{Boolean => JBool}
import java.util.{Set => JSet}
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

class JDBCUpsertOutputFormat(
    private val userName: String,
    private val password: String,
    private val driverName: String,
    private val dbURL: String,
    private val tableName: String,
    private val primaryKey: Array[String],
    private val uniqueKeys: JSet[JSet[String]])
  extends RichOutputFormat[JTuple2[JBool, Row]]
  with Logging {

  override def configure(parameters: Configuration): Unit = {

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {

  }

  override def close(): Unit = {

  }


  override def writeRecord(record: JTuple2[JBool, Row]): Unit = {
    if (record.f0) {
      // write upsert record

    } else {
      // write delete record
    }
  }

}

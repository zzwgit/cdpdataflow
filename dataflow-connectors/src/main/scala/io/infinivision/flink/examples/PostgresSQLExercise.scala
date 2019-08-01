package io.infinivision.flink.examples

import io.infinivision.flink.connectors.postgres.PostgresTableSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.factories.csv.CsvTableFactory
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.TableProperties
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.dataformat.{BaseRow, BinaryString}

import scala.collection.mutable
import scala.collection.JavaConverters._

object PostgresSQLExercise {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    // create csv probe table
    val csvPath = "D:\\test-files\\in\\train10.csv"
    val uniqueKeys = Set(
      Set("aid").asJava
    ).asJava
    val probeSource = CsvTableSource.builder()
      .path(csvPath)
      .field("aid", DataTypes.STRING)
      .field("uid", DataTypes.STRING)
      .field("label", DataTypes.STRING)
      .enableEmptyColumnAsNull()
      //      .uniqueKeys(Collections.singleton(Collections.singleton("aid")))
      .uniqueKeys(uniqueKeys)
      .ignoreFirstLine()
      .build()
    tEnv.registerTableSource("train", probeSource)

    // create build side table
    // build side table should at least one index
    // the join condition should contain at least one index
    val properties = mutable.Map[String, String]()
    properties += (("username", "postgres"))
    properties += (("password", "123456"))
    properties += (("tablename", "adfeature"))
    properties += (("dburl", "jdbc:postgresql://172.19.0.108:25432/postgres"))
    properties += (("mode", "async"))
    properties += (("Cache", "NONE"))
    properties += (("CacheTTLms", "3600000"))
    properties += (("asynctimeout", "100000"))
    properties += (("bufferCapacity", "100"))

    val tableProperties = new TableProperties
    tableProperties.putProperties(properties.asJava)
    val buildSource = TableExerciseUtils
      .createPostgresTableSource(tableProperties)
      .asInstanceOf[PostgresTableSource]
    tEnv.registerTableSource("adfeature", buildSource)

    // create csv table sink
    val csvSinkProperties = mutable.Map[String, String]()
    csvSinkProperties += (("path", "D:\\test-files\\out\\train_join_output.csv"))
    csvSinkProperties += (("updateMode", "append"))
    val csvSinkTableProperties = new TableProperties
    csvSinkTableProperties.putProperties(csvSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "aid", "uid", "advertiser"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING
    )
    val richSchema = new RichTableSchema(columnNames, columnTypes)
    csvSinkTableProperties.putSchemaIntoProperties(richSchema)
    val csvTableFactory = new CsvTableFactory
    val csvAppendTableSink = csvTableFactory.createStreamTableSink(csvSinkTableProperties.toMap)
    tEnv.registerTableSink("output", csvAppendTableSink)

    // build SQL
    val sql =
      """
        |SELECT
        |p.aid, p.uid, b.advertiser_id
        |FROM train AS p
        |INNER JOIN
        |adfeature FOR SYSTEM_TIME AS OF PROCTIME() AS b
        |ON p.aid = b.aid
      """.stripMargin

    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    result.print()
    env.execute("Temporal Table Join Exercise")
  }
}

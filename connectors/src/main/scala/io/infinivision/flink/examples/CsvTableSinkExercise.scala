package io.infinivision.flink.examples

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.factories.csv.CsvTableFactory
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.TableProperties

import scala.collection.mutable
import scala.collection.JavaConverters._

object CsvTableSinkExercise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    // create csv probe table
    val csvPath = "/Users/hongtaozhang/Downloads/train1w.csv"
    val probeSource = CsvTableSource.builder()
      .path(csvPath)
      .field("aid", DataTypes.STRING)
      .field("uid", DataTypes.STRING)
      .field("label", DataTypes.STRING)
      .enableEmptyColumnAsNull()
      .ignoreFirstLine()
      .build()
    tEnv.registerTableSource("train", probeSource)

    // create csv upsert table sink
    val csvSinkProperties = mutable.Map[String, String]()
    csvSinkProperties += (("path", "/Users/hongtaozhang/Downloads/train_retract_output.csv"))
    csvSinkProperties += (("updatemode", "retract"))
    val csvSinkTableProperties = new TableProperties
    csvSinkTableProperties.putProperties(csvSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "aid", "count"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.LONG
    )
    val richSchema = new RichTableSchema(columnNames, columnTypes)
    csvSinkTableProperties.putSchemaIntoProperties(richSchema)
    val csvTableFactory = new CsvTableFactory
    val csvTableSink = csvTableFactory.createStreamTableSink(csvSinkTableProperties.toMap)
    tEnv.registerTableSink("output", csvTableSink)

    // sql update
    val sql =
      """
        |INSERT INTO output
        |SELECT aid, count(uid) as cnt
        |FROM train
        |GROUP BY aid
      """.stripMargin
    tEnv.sqlUpdate(sql)

    env.execute("csv table sink exercise")
  }
}

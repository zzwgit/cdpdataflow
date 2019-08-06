package io.infinivision.flink.examples

import io.infinivision.flink.udfs.json.array.CollectToJsonAggFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.factories.csv.CsvTableFactory
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.TableProperties
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable

object CollectToJsonAggSQLExercise {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    // build source begin*****************************************************************************************
    val csvPath = "D:\\test-files\\in\\train10.csv"
    val uniqueKeys = Set(
      Set("aid").asJava
    ).asJava
    val probeSource = CsvTableSource.builder()
      .path(csvPath)
      .field("aid", DataTypes.STRING)
      .field("uid", DataTypes.LONG)
      .field("label", DataTypes.STRING)
      .enableEmptyColumnAsNull()
      //      .uniqueKeys(Collections.singleton(Collections.singleton("aid")))
      .uniqueKeys(uniqueKeys)
      .ignoreFirstLine()
      .build()
    tEnv.registerTableSource("train", probeSource)
    // build source end*****************************************************************************************


    // create csv table sink
    val csvSinkProperties = mutable.Map[String, String]()
    csvSinkProperties += (("path", "D:\\test-files\\out\\collect_toj_son.csv"))
    csvSinkProperties += (("updateMode", "append"))
    val csvSinkTableProperties = new TableProperties
    csvSinkTableProperties.putProperties(csvSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "aid", "seq_num"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.LONG
    )
    val richSchema = new RichTableSchema(columnNames, columnTypes)
    csvSinkTableProperties.putSchemaIntoProperties(richSchema)
    val csvTableFactory = new CsvTableFactory
    val csvAppendTableSink = csvTableFactory.createStreamTableSink(csvSinkTableProperties.toMap)
    tEnv.registerTableSink("output", csvAppendTableSink)

    // register function
    // tEnv.registerFunction("idGenerator",new IdGeneratorUDFWithBatch())
    tEnv.registerFunction("collectToJson", new CollectToJsonAggFunction())
    // build SQL

    // udf方式
        val sql =
          """
            |SELECT
            |p.aid, collectToJson(p.uid) as seq_num
            |FROM train AS p
            |group by p.aid
          """.stripMargin

    //udtf方式
    //    val sql =
    //      """
    //        |SELECT
    //        |    p.aid, p.uid,t.uuid as seq_id
    //        |    FROM
    //        |    train as p
    //        |    LEFT JOIN LATERAL TABLE (idGenerator(uid)) as t (
    //        |      uuid
    //        |    ) ON TRUE
    //      """.stripMargin


    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
    result.print()
    env.execute("Temporal Table Join Exercise")
  }
}

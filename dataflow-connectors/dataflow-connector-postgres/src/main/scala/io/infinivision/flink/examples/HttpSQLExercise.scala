package io.infinivision.flink.examples

import io.infinivision.flink.connectors.http.HttpTableFactory
import io.infinivision.flink.udfs.IdGeneratorUDTFWithBatch
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

object HttpSQLExercise {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    // build source begin*****************************************************************************************
    val csvPath = "D:\\test-files\\in\\train.csv"
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
    // build source end*****************************************************************************************


    //build temporal begin*****************************************************************************************
    val properties = mutable.Map[String, String]()
    properties += (("request.url", "http://172.19.0.108:18080/v1/id/next"))
    //properties += (("request.url", "http://10.126.144.141:8080/v1/id/next"))
    properties += (("mode", "async"))
    properties += (("asynctimeout", "20000"))
    properties += (("bufferCapacity", "5"))

    val sourcetableProperties = new TableProperties
    sourcetableProperties.putProperties(properties.asJava)

    val sourcecolumnNames: Array[String] = Array(
      "id",
      "seq_id"
    )

    val sourcecolumnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.LONG
    )

    val sourcerichSchema = new RichTableSchema(sourcecolumnNames, sourcecolumnTypes)
    sourcerichSchema.setPrimaryKey("id")
    sourcetableProperties.putSchemaIntoProperties(sourcerichSchema)

    val tableFactory = new HttpTableFactory
    val buildSource = tableFactory.createStreamTableSource(sourcetableProperties.toKeyLowerCase.toMap)
    tEnv.registerTableSource("id_generator", buildSource)
    //build source end*****************************************************************************************


    // create csv table sink
    val csvSinkProperties = mutable.Map[String, String]()
    csvSinkProperties += (("path", "D:\\test-files\\out\\id_generator.csv"))
    csvSinkProperties += (("updateMode", "append"))
    val csvSinkTableProperties = new TableProperties
    csvSinkTableProperties.putProperties(csvSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "aid", "uid", "seq_id"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
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
    tEnv.registerFunction("idGenerator", new IdGeneratorUDTFWithBatch())
    // build SQL
    //维表方式
    val sql =
      """
        |SELECT
        |p.aid, p.uid, b.seq_id
        |FROM train AS p
        |LEFT JOIN
        |id_generator FOR SYSTEM_TIME AS OF PROCTIME() AS b
        |ON p.aid = b.id
      """.stripMargin

    //udf方式
    //    val sql =
    //      """
    //        |SELECT
    //        |p.aid, p.uid, idGenerator(uid,10) asvseq_id
    //        |FROM train AS p
    //      """.stripMargin

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


    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    result.print()
    env.execute("Temporal Table Join Exercise")
  }
}

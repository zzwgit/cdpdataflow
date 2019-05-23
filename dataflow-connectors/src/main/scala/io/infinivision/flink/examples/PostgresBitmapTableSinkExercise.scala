package io.infinivision.flink.examples

import io.infinivision.flink.connectors.postgres.PostgresTableFactory
import io.infinivision.flink.udfs.LongCollectListFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.TableProperties

import org.apache.flink.api.scala._
import scala.collection.JavaConverters._
import scala.collection.mutable

object PostgresBitmapTableSinkExercise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)

    // create csv probe table
    val csvPath = "/Users/hongtaozhang/Downloads/train10.csv"
    val probeSource = CsvTableSource.builder()
      .path(csvPath)
      .field("aid", DataTypes.LONG)
      .field("uid", DataTypes.INT)
      .field("label", DataTypes.STRING)
      .enableEmptyColumnAsNull()
      .ignoreFirstLine()
      .build()
    tEnv.registerTableSource("train", probeSource)

    // create postgres upsert table sink
    val postgresSinkProperties = mutable.Map[String, String]()
    postgresSinkProperties += (("updatemode", "upsert"))
    postgresSinkProperties += (("version", "9.5"))
    postgresSinkProperties +=(("username", "gpadmin"))
    postgresSinkProperties += (("password", "Welcome123@"))
    postgresSinkProperties += (("tablename", "flink_gp_bitmap"))
    postgresSinkProperties += (("bitmapfield", "user_list"))
    postgresSinkProperties += (("dburl", "jdbc:postgresql://172.19.0.108:5432/db_dmx_stage"))
    val postgresSinkTableProperties = new TableProperties
    postgresSinkTableProperties.putProperties(postgresSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "uid", "user_list"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.INT,
      DataTypes.BYTE_ARRAY
    )

    val richSchema = new RichTableSchema(columnNames, columnTypes)
    val uniqueKeys = List(
      List("uid").asJava
    ).asJava
    richSchema.setUniqueKeys(uniqueKeys)
    postgresSinkTableProperties.putSchemaIntoProperties(richSchema)
    val postgresTableFactory = new PostgresTableFactory
    val postgresTableSink = postgresTableFactory.createStreamTableSink(postgresSinkTableProperties.toMap)
    tEnv.registerTableSink("output", postgresTableSink)
    tEnv.registerFunction("long_collect_list", new LongCollectListFunction)

    // sql update
    val sql =
      """
        |INSERT INTO output
        |SELECT uid, long_collect_list(aid)
        |FROM train
        |GROUP BY uid
      """.stripMargin
    tEnv.sqlUpdate(sql)

    env.execute("postgres table sink exercise")
  }

}

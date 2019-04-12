package io.infinivision.flink.examples

import io.infinivision.flink.connectors.postgres.PostgresTableFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.TableProperties

import scala.collection.mutable
import scala.collection.JavaConverters._

object PostgresTableSinkExercise {
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

    // create postgres upsert table sink
    val postgresSinkProperties = mutable.Map[String, String]()
    postgresSinkProperties += (("updatemode", "upsert"))
    postgresSinkProperties += (("version", "9.6"))
    postgresSinkProperties +=(("username", "postgres"))
    postgresSinkProperties += (("password", "123456"))
    postgresSinkProperties += (("tablename", "train_output"))
    postgresSinkProperties += (("dburl", "jdbc:postgresql://localhost:5432/postgres"))
    val postgresSinkTableProperties = new TableProperties
    postgresSinkTableProperties.putProperties(postgresSinkProperties.asJava)

    val columnNames: Array[String] = Array(
      "aid", "uid", "label"
    )
    val columnTypes: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.STRING,
      DataTypes.STRING
    )

    val richSchema = new RichTableSchema(columnNames, columnTypes)
    val uniqueKeys = List(
      List("aid", "uid").asJava
    ).asJava
//    richSchema.setPrimaryKey("aid")
    richSchema.setUniqueKeys(uniqueKeys)
    postgresSinkTableProperties.putSchemaIntoProperties(richSchema)
    val postgresTableFactory = new PostgresTableFactory
    val postgresTableSink = postgresTableFactory.createStreamTableSink(postgresSinkTableProperties.toMap)
    tEnv.registerTableSink("output", postgresTableSink)

    // sql update
    val sql =
      """
        |INSERT INTO output
        |SELECT aid, uid, label
        |FROM train
      """.stripMargin
    tEnv.sqlUpdate(sql)

    env.execute("postgres table sink exercise")
  }
}

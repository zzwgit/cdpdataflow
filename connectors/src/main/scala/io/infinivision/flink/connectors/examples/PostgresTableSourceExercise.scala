package io.infinivision.flink.connectors.examples

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.util.TableProperties
import org.apache.flink.types.Row
import scala.collection.JavaConverters._

import scala.collection.mutable

object PostgresTableSourceExercise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // register table source
    val properties = mutable.Map[String, String]()
    properties +=(("username", "postgres"))
    properties += (("password", "123456"))
    properties += (("tablename", "adfeature"))
    properties += (("dburl", "jdbc:postgresql://localhost:5432/postgres"))
    properties += (("mode", "async"))
    properties += (("timeout", "10000"))
    properties += (("bufferCapacity", "100"))

    val tableProperties = new TableProperties
    tableProperties.putProperties(properties.asJava)
    tEnv.registerTableSource("adfeature",
      TableExerciseUtils.createPostgresTableSource(tableProperties))
    env.setParallelism(1)
    val sql =
      """
        | SELECT aid FROM adfeature
      """.stripMargin

    val result = tEnv.sqlQuery(sql).toAppendStream[Row]
    result.print()
    env.execute("Postgres Table Source Exercise")
  }
}


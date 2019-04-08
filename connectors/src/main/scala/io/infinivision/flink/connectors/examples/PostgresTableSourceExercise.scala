package io.infinivision.flink.connectors.examples

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row

object PostgresTableSourceExercise {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerTableSource("adfeature", TableExerciseUtils.createPostgresTableSource())
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


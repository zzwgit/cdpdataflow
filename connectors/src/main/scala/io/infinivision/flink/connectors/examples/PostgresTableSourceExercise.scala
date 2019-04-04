package io.infinivision.flink.connectors.examples

import io.infinivision.flink.connectors.postgres.PostgresTableFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.api.{RichTableSchema, TableEnvironment}
import org.apache.flink.table.util.TableProperties
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._


class PostgresTableSourceExercise {

}

object PostgresTableSourceExercise {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)

  val columnNames: Array[String] = Array(
    "aid",
    "advertiser_id",
    "campaign_id",
    "creative_id",
    "creative_size",
    "category_id",
    "product_id",
    "product_type"
  )

  val columnTypes: Array[InternalType] = Array(
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING,
    DataTypes.STRING
  )
  val richSchema = new RichTableSchema(columnNames, columnTypes)

  def main(args: Array[String]): Unit = {
    val tableProperties = new TableProperties
    tableProperties.property("username", "postgres")
    tableProperties.property("password", "123456")
    tableProperties.property("tablename", "adfeature")
    tableProperties.property("dburl", "jdbc:postgresql://localhost:5432/postgres")
    tableProperties.property("username", "postgres")
    tableProperties.putSchemaIntoProperties(richSchema)

    val tableFactory = new PostgresTableFactory
    val tableSource = tableFactory.createStreamTableSource(tableProperties.toKeyLowerCase.toMap)

    tEnv.registerTableSource("adfeature", tableSource)
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


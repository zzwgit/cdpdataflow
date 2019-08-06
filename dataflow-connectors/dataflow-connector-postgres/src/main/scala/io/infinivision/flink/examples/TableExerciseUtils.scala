package io.infinivision.flink.examples

import io.infinivision.flink.connectors.postgres.PostgresTableFactory
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.table.util.TableProperties

object TableExerciseUtils {
  def createPostgresTableSource(tableProperties: TableProperties): StreamTableSource[BaseRow] = {
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
    richSchema.setPrimaryKey("aid")
    tableProperties.putSchemaIntoProperties(richSchema)

    val tableFactory = new PostgresTableFactory
    tableFactory.createStreamTableSource(tableProperties.toKeyLowerCase.toMap)
  }
}

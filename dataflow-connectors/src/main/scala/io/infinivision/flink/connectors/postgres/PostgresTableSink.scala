package io.infinivision.flink.connectors.postgres

import io.infinivision.flink.connectors.jdbc.{JDBCBaseOutputFormat, JDBCTableSink, JDBCTableSinkBuilder}

class PostgresTableSink (
  outputFormat: JDBCBaseOutputFormat)
extends JDBCTableSink(outputFormat) {

}


object PostgresTableSink  {
  class Builder extends JDBCTableSinkBuilder {

    private var bitmapField: Option[String] = None

    def bitmapField(bitmapField: Option[String]): JDBCTableSinkBuilder = {
      this.bitmapField = bitmapField
      this
    }

    override def build(): JDBCTableSink = {
      // check condition
      if (schema.isEmpty) {
        throw new IllegalArgumentException("table schema can not be null")
      }

      val outputFormat: JDBCBaseOutputFormat = if (updateMode == "append") {
        new PostgresAppendOutputFormat(
          userName,
          password,
          driverName,
          driverVersion,
          dbURL,
          tableName,
          schema.get.getColumnNames,
          schema.get.getColumnTypes
        )
      } else {

        // validate index keys
        val uniqueIndex = if (primaryKeys.isEmpty && uniqueKeys.isEmpty) {
          throw new IllegalArgumentException("JDBCUpsertTableSink should at least contain one primary key or one unique index")
        } else if (primaryKeys.isDefined){
          primaryKeys.get
        } else {
          if(uniqueKeys.get.size != 1) {
            throw new IllegalArgumentException("JDBCUpsertTableSink should contain only one unique index")
          }

          if (uniqueKeys.get.size == schema.get.getColumnNames.length) {
            throw new IllegalArgumentException("JDBCUpsertTableSink unique key size should less than total column size")
          }

          uniqueKeys.get.iterator.next
        }

        new PostgresUpsertOutputFormat(
          userName,
          password,
          driverName,
          driverVersion,
          dbURL,
          tableName,
          schema.get.getColumnNames,
          schema.get.getColumnTypes,
          bitmapField,
          uniqueIndex
        )
      }

      new PostgresTableSink(outputFormat)
    }

  }

  def builder(): Builder = new Builder()
}
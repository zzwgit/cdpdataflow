package io.infinivision.flink.connectors.hbase

import java.lang.{Integer => JInteger}
import java.util

import io.infinivision.flink.connectors.utils.CacheConfig
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.functions.{FunctionContext, TableFunction}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.util.{Logging, TableProperties}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

/**
  * only used in cache-all scenario
  */
class HBaseLookupFunction(tableProperties: TableProperties,
                          tableSchema: RichTableSchema,
                          hbaseTableName: String,
                          hbaseSchema: HBaseTableSchemaV2,
                          rowKeySourceIndex: Int,
                          qualifierSourceIndexes: util.List[JInteger],
                          hbaseConfiguration: Configuration,
                          cacheConfig: CacheConfig)
    extends TableFunction[BaseRow]
    with Logging {
  private val asyncLookupFunction: HBaseAsyncLookupFunction =
    new HBaseAsyncLookupFunction(
      tableProperties,
      tableSchema,
      hbaseTableName,
      hbaseSchema,
      rowKeySourceIndex,
      qualifierSourceIndexes,
      null,
      cacheConfig
    )

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    this.asyncLookupFunction.open(context)
  }

  class SimpleResultFutureImpl[A] extends ResultFuture[A] {

    var collection: util.Collection[A] = _

    def reset(): Unit = {
      this.collection = null
    }

    override def complete(result: util.Collection[A]): Unit = {
      collection = result
    }

    // never called
    override def completeExceptionally(throwable: Throwable): Unit = ???
  }

  def eval(rowKey: Object): Unit = {
    val resultFuture: SimpleResultFutureImpl[BaseRow] =
      new SimpleResultFutureImpl()
    this.asyncLookupFunction.eval(resultFuture, rowKey)
    resultFuture.collection.asScala.foreach(e => collect(e))
  }

  override def close(): Unit = {
    super.close()
    this.asyncLookupFunction.close()
  }
}

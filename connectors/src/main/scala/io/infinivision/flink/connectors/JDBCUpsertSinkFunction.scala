package io.infinivision.flink.connectors

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import java.lang.{Boolean => JBool}
import java.util

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

class JDBCUpsertSinkFunction(outputFormat: JDBCUpsertOutputFormat)
  extends RichSinkFunction[JTuple2[JBool, Row]]
  with ListCheckpointed[Row]
  with Logging{

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ctx = getRuntimeContext
    outputFormat.setRuntimeContext(ctx)
    outputFormat.open(ctx.getIndexOfThisSubtask, ctx.getNumberOfParallelSubtasks)
  }

  override def close(): Unit = {
    outputFormat.close()
    super.close()
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Row] = {
    outputFormat.flush()
    new util.ArrayList[Row]()
  }

  override def restoreState(state: util.List[Row]): Unit = {}


  override def invoke(record: JTuple2[JBool, Row],
                      context: SinkFunction.Context[_]): Unit = {
    outputFormat.writeRecord(record)
  }


}

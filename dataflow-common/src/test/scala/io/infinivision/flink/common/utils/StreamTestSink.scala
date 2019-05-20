package io.infinivision.flink.common.utils

import java.util.TimeZone
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.Types
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.dataformat.{BaseRow, GenericRow}
import org.apache.flink.table.dataformat.util.BaseRowUtil
import org.apache.flink.table.runtime.conversion.DataStructureConverters
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.types.Row

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object StreamTestSink {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private[utils] val idCounter: AtomicInteger = new AtomicInteger(0)

  private[utils] val globalResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalRetractResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalUpsertResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, mutable.Map[String, String]]]

  private[utils] def getNewSinkId: Int = {
    val idx = idCounter.getAndIncrement()
    this.synchronized{
      globalResults.put(idx, mutable.HashMap.empty[Int, ArrayBuffer[String]])
      globalRetractResults.put(idx, mutable.HashMap.empty[Int, ArrayBuffer[String]])
      globalUpsertResults.put(idx, mutable.HashMap.empty[Int, mutable.Map[String, String]])
    }
    idx
  }

  def clear(): Unit = {
    globalResults.clear()
    globalRetractResults.clear()
    globalUpsertResults.clear()
  }
}

abstract class AbstractExactlyOnceSink[T] extends RichSinkFunction[T] with CheckpointedFunction {
  protected var resultsState: ListState[String] = _
  protected var localResults: ArrayBuffer[String] = _
  protected val idx: Int = StreamTestSink.getNewSinkId

  protected var globalResults: mutable.Map[Int, ArrayBuffer[String]]= _
  protected var globalRetractResults: mutable.Map[Int, ArrayBuffer[String]] = _
  protected var globalUpsertResults: mutable.Map[Int, mutable.Map[String, String]] = _

  override def initializeState(context: FunctionInitializationContext): Unit = {
    resultsState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[String]("sink-results", Types.STRING))

    localResults = mutable.ArrayBuffer.empty[String]

    if (context.isRestored) {
      for (value <- resultsState.get().asScala) {
        localResults += value
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized(
      StreamTestSink.globalResults(idx) += (taskId -> localResults)
    )
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    resultsState.clear()
    for (value <- localResults) {
      resultsState.add(value)
    }
  }

  protected def clearAndStashGlobalResults(): Unit = {
    if (globalResults == null) {
      StreamTestSink.synchronized{
        globalResults = StreamTestSink.globalResults.remove(idx).get
        globalRetractResults = StreamTestSink.globalRetractResults.remove(idx).get
        globalUpsertResults = StreamTestSink.globalUpsertResults.remove(idx).get
      }
    }
  }

  protected def getResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalResults.foreach {
      case (_, list) => result ++= list
    }
    result.toList
  }
}

final class TestingAppendSink extends AbstractExactlyOnceSink[Row] {
  def invoke(value: Row): Unit = localResults += value.toString
  def getAppendResults: List[String] = getResults
}


final class TestingAppendBaseRowSink(rowTypeInfo: BaseRowTypeInfo)
  extends AbstractExactlyOnceSink[BaseRow] {
  def invoke(value: BaseRow): Unit = localResults += baseRowToString(value, rowTypeInfo)
  def getAppendResults: List[String] = getResults
  def baseRowToString(value: BaseRow, rowTypeInfo: BaseRowTypeInfo): String = {
    val config = new ExecutionConfig
    val fieldTypes = rowTypeInfo.getFieldTypes
    val fieldSerializers = fieldTypes.map(_.createSerializer(config))
    BaseRowUtil.toGenericRow(value, fieldTypes, fieldSerializers).toString
  }
}

class TestingRetractSink extends AbstractExactlyOnceSink[(Boolean, Row)] {
  protected var retractResultsState: ListState[String] = _
  protected var localRetractResults: ArrayBuffer[String] = _

  override def initializeState(context: FunctionInitializationContext): Unit = {
    super.initializeState(context)
    retractResultsState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[String]("sink-retract-results", Types.STRING))

    localRetractResults = mutable.ArrayBuffer.empty[String]

    if (context.isRestored) {
      for (value <- retractResultsState.get().asScala) {
        localRetractResults += value
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized{
      StreamTestSink.globalRetractResults(idx) += (taskId -> localRetractResults)
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    retractResultsState.clear()
    for (value <- localRetractResults) {
      retractResultsState.add(value)
    }
  }

  def invoke(v: (Boolean, Row)): Unit = {
    this.synchronized {
      val tupleString = v.toString()
      localResults += tupleString
      val rowString = v._2.toString
      if (v._1) {
        localRetractResults += rowString
      } else {
        val index = localRetractResults.indexOf(rowString)
        if (index >= 0) {
          localRetractResults.remove(index)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }

  def getRawResults: List[String] = getResults

  def getRetractResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalRetractResults.foreach {
      case (_, list) => result ++= list
    }
    result.toList
  }
}

final class TestingUpsertSink(keys: Array[Int])
  extends AbstractExactlyOnceSink[BaseRow] {

  private var upsertResultsState: ListState[String] = _
  private var localUpsertResults: mutable.Map[String, String] = _
  private var fieldTypes: Array[DataType] = _

  def configureTypes(fieldTypes: Array[DataType]): Unit = {
    this.fieldTypes = fieldTypes
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    super.initializeState(context)
    upsertResultsState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[String]("sink-upsert-results", Types.STRING))

    localUpsertResults = mutable.HashMap.empty[String, String]

    if (context.isRestored) {
      var key: String = null
      var value: String = null
      for (entry <- upsertResultsState.get().asScala) {
        if (key == null) {
          key = entry
        } else {
          value = entry
          localUpsertResults += (key -> value)
          key = null
          value = null
        }
      }
      if (key != null) {
        throw new RuntimeException("The resultState is corrupt.")
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized{
      StreamTestSink.globalUpsertResults(idx) += (taskId -> localUpsertResults)
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    upsertResultsState.clear()
    for ((key, value) <- localUpsertResults) {
      upsertResultsState.add(key)
      upsertResultsState.add(value)
    }
  }

  def invoke(row: BaseRow): Unit = {

    val wrapRow = new GenericRow(2)
    wrapRow.update(0, BaseRowUtil.isAccumulateMsg(row))
    wrapRow.update(1, row)
    val converter = DataStructureConverters.createToExternalConverter(
      DataTypes.createTupleType(DataTypes.BOOLEAN, DataTypes.createRowType(fieldTypes: _*)))
    val v = converter.apply(wrapRow).asInstanceOf[JTuple2[Boolean, Row]]

    val tupleString = v.toString
    localResults += tupleString
    val keyString = Row.project(v.f1, keys).toString
    if (v.f0) {
      localUpsertResults += (keyString -> v.f1.toString)
    } else {
      val oldValue = localUpsertResults.remove(keyString)
      if (oldValue.isEmpty) {
        throw new RuntimeException("Tried to delete a value that wasn't inserted first. " +
          "This is probably an incorrectly implemented test. " +
          "Try to set the parallelism of the sink to 1.")
      }
    }
  }

  def getRawResults: List[String] = getResults

  def getUpsertResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalUpsertResults.foreach {
      case (_, map) => map.foreach(result += _._2)
    }
    result.toList
  }
}

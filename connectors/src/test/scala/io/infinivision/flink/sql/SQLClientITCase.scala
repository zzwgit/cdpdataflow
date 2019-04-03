package io.infinivision.flink.sql

import org.apache.flink.client.cli.util.DummyCustomCommandLine
import org.apache.flink.client.program.ClusterClient
import org.apache.flink.configuration.{ConfigConstants, Configuration, TaskManagerOptions, WebOptions}
import org.apache.flink.table.client.gateway.{Executor, SessionContext, TypedResult}
import org.apache.flink.table.client.gateway.local.LocalExecutor
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil
import org.apache.flink.test.util.MiniClusterResource
import org.junit.{AfterClass, Before, BeforeClass, ClassRule}
import org.junit.rules.TemporaryFolder

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.flink.util.TestLogger

class SQLClientITCase extends TestLogger {
  val DEFAULTS_ENVIRONMENT_FILE = "test-sql-client-defaults.yaml"

  def createDefaultExecutor[T](clusterClient: ClusterClient[T]): LocalExecutor = {
    val replaceVars = new mutable.HashMap[String, String]()
    replaceVars.+=(("$VAR_EXECUTION_TYPE", "streaming"))
    replaceVars.+=(("$VAR_UPDATE_MODE", "update-mode: append"))
    replaceVars.+=(("$VAR_RESULT_MODE", "changelog"))
    replaceVars.+=(("$VAR_MAX_ROWS", "100"))
    val commandLine = new DummyCustomCommandLine(clusterClient)
    new LocalExecutor(
      EnvironmentFileUtil.parseModified(DEFAULTS_ENVIRONMENT_FILE, replaceVars),
      List(),
      clusterClient.getFlinkConfiguration,
      commandLine
    )
  }

  def retriveChangeLogResult(executor: Executor, session: SessionContext, resultId: String): List[String] = {
    var actualResult: List[String] = List()
    while (true) {
      Thread.sleep(50)
      val result = executor.retrieveResultChanges(session, resultId)
      result.getType match {
        case TypedResult.ResultType.PAYLOAD =>
          result.getPayload.foreach { change =>
            actualResult = actualResult :+ change.toString
          }
        case TypedResult.ResultType.EOS => return actualResult
        case TypedResult.ResultType.EMPTY =>
      }
    }
    actualResult
  }

  @Before
  def before(): Unit = {
    log.debug("=========before======")
  }

}

object SQLClientITCase {

  val NUM_TMS = 2
  val NUM_SLOTS_PER_TM = 2
  val tempFolder = new TemporaryFolder()

  var clusterClient: ClusterClient[_] = _

  def getConfig: Configuration = {

    val config = new Configuration()
    config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4 * 1024 * 1024)
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS)
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM)
    config.setBoolean(WebOptions.SUBMIT_ENABLE, false)
    config
  }

  @ClassRule
  val miniClusterResource = new MiniClusterResource(
    new MiniClusterResource.MiniClusterResourceConfiguration(
      getConfig,
      NUM_TMS,
      NUM_SLOTS_PER_TM),
    true
  )

  @BeforeClass
  def setup(): Unit = {
    miniClusterResource.before()
  }

  @AfterClass
  def teardown(): Unit = {
    miniClusterResource.after()
  }
}
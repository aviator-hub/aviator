package io.github.aviatorhub.aviator.app

import io.github.aviatorhub.aviator.app.conf.{AviatorConfManager, AviatorJobConf}
import io.github.aviatorhub.aviator.app.constant.{CheckpointStateBackend, RunningMode}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigOption, PipelineOptions}
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.util.Preconditions.checkNotNull

import java.lang
import java.util.concurrent.TimeUnit

/**
 * A abstract flink job super class, help user to pay attention to business logical only.
 *
 * @author jie mei
 * @param jobConf
 * @param tableEnv
 * @since 2021/10/22
 */
class AviatorSqlApp(var jobConf: AviatorJobConf = null,
                    var tableEnv: TableEnvironment = null) {

  protected def init(args: Array[String]): Unit = {
    initConf()
    prepareTableEnv()
    applyConf()
  }


  private def initConf(): Unit = {
    AviatorConfManager.loadAviatorConf()
    jobConf = AviatorConfManager.applyJobDeclare(this.getClass)
  }

  /**
   * Prepare Table Environment for user to creating table and executing sql.
   */
  private def prepareTableEnv(): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.enableCheckpointing(jobConf.getCheckpointInterval, jobConf.getCheckpointingMode)

    if (RunningMode.PRODUCTION.equals(jobConf.getRunningMode)) {
      streamEnv.setRestartStrategy(RestartStrategies.failureRateRestart(3,
        Time.of(10, TimeUnit.MINUTES),
        Time.of(1, TimeUnit.MINUTES)))

      streamEnv.getCheckpointConfig.setCheckpointStorage(jobConf.getCheckpointPath)
      jobConf.getCheckpointStateBackend match {
        case CheckpointStateBackend.FILE =>
          streamEnv.setStateBackend(new HashMapStateBackend)
        case CheckpointStateBackend.ROCKSDB =>
          streamEnv.setStateBackend(new EmbeddedRocksDBStateBackend)
        case _ => // do nothing
      }

    }
    // TODO support batch mode
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner().inStreamingMode().build()
    tableEnv = StreamTableEnvironment.create(streamEnv, settings)
  }

  private def applyConf(): Unit = {
    // set job name
    val jobName = if (StringUtils.isBlank(jobConf.getJobName)) {
      this.getClass.getCanonicalName.replace("$", "")
    } else {
      jobConf.getJobName
    }
    setConfig(PipelineOptions.NAME, getNameWithVersion(jobName))

    if (jobConf.isDistinctAggSplit) {
      enableConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED)
    }

    if (jobConf.isMiniBatch) {
      enableConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
      setConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "60 s")
      setConfig(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, "10000")
    }

    if (jobConf.isTwoPhaseAgg) {
      setConfig(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE")
    }

  }

  private def getNameWithVersion(name: String): String = {
    jobConf.getVersion match {
      case 0 => name
      case _ => s"${name}_${jobConf.getVersion}"
    }
  }

  private def setConfig[T](option: ConfigOption[T], value: String): Unit = {
    tableEnv.getConfig.getConfiguration.setString(option.key(), value)
  }

  private def enableConfig(option: ConfigOption[lang.Boolean]): Unit = {
    tableEnv.getConfig.getConfiguration.setBoolean(option.key(), true)
  }

  protected def createPrintTable(schemaStr: String): Unit = {
    checkNotNull(tableEnv)
    tableEnv.executeSql(
      s"""
         | $schemaStr WITH (
         |  'connector' = 'print'
         | )
         |""".stripMargin)

  }
}

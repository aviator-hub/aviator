package io.github.aviatorhub.aviator.app

import io.github.aviatorhub.aviator.app.conf.{AviatorConfManager, AviatorJobConf}
import io.github.aviatorhub.aviator.app.constant.{CheckpointStateBackend, ConnType, RunningMode}
import io.github.aviatorhub.aviator.app.table.{ClickHouseTableBuilder, ElasticSearchTableBuilder, HbaseTableBuilder, JdbcTableBuilder, RedisTableBuilder}
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
import org.apache.flink.util.Preconditions
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

  // ================================================================
  // == MYSQL TABLE
  // ================================================================
  protected def createMysqlTable[E >: Enumeration](schemaStr: String,
                                                   table: String,
                                                   mysql: E): Unit = {
    createMysqlTable(schemaStr, null, table, mysql)
  }

  protected def createMysqlTable[E >: Enumeration](schemaStr: String,
                                                   database: String,
                                                   table: String,
                                                   mysql: E): Unit = {
    mysqlTableBuilder(schemaStr, table, mysql)
      .database(database).build()
  }

  protected def mysqlTableBuilder[E >: Enumeration](schemaStr: String,
                                                    table: String,
                                                    mysql: E): JdbcTableBuilder = {
    val mysqlConf = AviatorConfManager.getConnConf(mysql.toString, ConnType.MYSQL)
    Preconditions.checkNotNull(mysqlConf)
    new JdbcTableBuilder(schemaStr, mysqlConf, tableEnv)
      .table(table)
      .driver("com.mysql.jdbc.Driver")
  }

  // ================================================================
  // == CLICKHOUSE TABLE
  // ================================================================
  protected def createClickhouseTable[E >: Enumeration](schemaStr: String,
                                                        table: String,
                                                        clickhouse: E): Unit = {
    createClickhouseTable(schemaStr, null, table, clickhouse)
  }

  protected def createClickhouseTable[E >: Enumeration](schemaStr: String,
                                                        database: String,
                                                        table: String,
                                                        clickhouse: E): Unit = {
    clickhouseTableBuilder(schemaStr, table, clickhouse)
      .database(database)
      .build()
  }

  protected def clickhouseTableBuilder[E >: Enumeration](schemaStr: String,
                                                         table: String,
                                                         clickhouse: E): ClickHouseTableBuilder = {
    val clickhouseConn = AviatorConfManager.getConnConf(clickhouse.toString, ConnType.CLICKHOUSE)
    Preconditions.checkNotNull(clickhouseConn)
    new ClickHouseTableBuilder(schemaStr, clickhouseConn, tableEnv)
      .table(table)
  }

  // ================================================================
  // == REDIS TABLE
  // ================================================================
  protected def createRedisTable[E >: Enumeration](schemaStr: String,
                                                   redis: E): Unit = {
    redisTableBuilder(schemaStr, redis).build()
  }

  protected def redisTableBuilder[E >: Enumeration](schemaStr: String,
                                                    redis: E): RedisTableBuilder = {
    val redisConn = AviatorConfManager.getConnConf(redis.toString, ConnType.REDIS)
    Preconditions.checkNotNull(redisConn)
    new RedisTableBuilder(schemaStr, redisConn, tableEnv)
  }

  // ================================================================
  // == HBASE TABLE
  // ================================================================
  protected def createHbaseTable[E >: Enumeration](schemaStr: String,
                                                   hbase: E): Unit = {
    hbaseTableBuilder(schemaStr, hbase).build()
  }

  protected def hbaseTableBuilder[E >: Enumeration](schemaStr: String,
                                                    hbase: E): HbaseTableBuilder = {
    val hbaseConn = AviatorConfManager.getConnConf(hbase.toString, ConnType.HBASE)
    Preconditions.checkNotNull(hbaseConn)
    new HbaseTableBuilder(schemaStr, hbaseConn, tableEnv)
  }

  // ================================================================
  // == ELASTICSEARCH TABLE
  // ================================================================
  protected def createElasticsearchTable[E >: Enumeration](schemaStr: String,
                                                           index: String,
                                                           elasticsearch: E): Unit = {
    elasticsearchTableBuilder(schemaStr, index, elasticsearch).build()
  }

  protected def elasticsearchTableBuilder[E >: Enumeration](schemaStr: String,
                                                            index: String,
                                                            elasticsearch: E): ElasticSearchTableBuilder = {
    val conn = AviatorConfManager.getConnConf(elasticsearch.toString, ConnType.ELASTICSEARCH)
    Preconditions.checkNotNull(conn)
    new ElasticSearchTableBuilder(schemaStr, conn, tableEnv).table(index)
  }
}

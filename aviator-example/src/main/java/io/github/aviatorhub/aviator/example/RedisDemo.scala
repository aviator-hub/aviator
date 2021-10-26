package io.github.aviatorhub.aviator.example

import io.github.aviatorhub.aviator.app.AviatorSqlApp
import io.github.aviatorhub.aviator.app.annotation.ContainerEnvDeclare
import io.github.aviatorhub.aviator.app.constant.ContainerEnv
import org.apache.flink.table.api.TableResult

@ContainerEnvDeclare(Array(ContainerEnv.REDIS))
object RedisDemo extends AviatorSqlApp() {

  def main(args: Array[String]): Unit = {
    runJob(args)
  }

  override def process(): TableResult = {
    createRedisTable(
      s"""
         | CREATE TABLE REDIS_SINK_DEMO (
         | `USER_ID` bigint,
         | `MESSAGE` varchar,
         | PRIMARY KEY (`USER_ID`) NOT ENFORCED
         | )
         |""".stripMargin, RedisCluster.TEST)

    tableEnv.executeSql(
      """
        | INSERT INTO REDIS_SINK_DEMO
        | VALUES (1, 'test'), (2, 'test2'), (1, 'test1')
        |""".stripMargin)
  }
}

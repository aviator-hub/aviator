package io.github.aviatorhub.aviator.example

import io.github.aviatorhub.aviator.app.AviatorSqlApp
import io.github.aviatorhub.aviator.app.annotation.ContainerEnvDeclare
import io.github.aviatorhub.aviator.app.constant.ContainerEnv
import org.apache.flink.api.common.JobStatus

@ContainerEnvDeclare(Array(ContainerEnv.REDIS))
object RedisDemo extends AviatorSqlApp() {

  def main(args: Array[String]): Unit = {
    init(args)
    createRedisTable(
      s"""
         | CREATE TABLE REDIS_SINK_DEMO (
         | `USER_ID` bigint,
         | `MESSAGE` varchar,
         | PRIMARY KEY (`USER_ID`) NOT ENFORCED
         | )
         |""".stripMargin, RedisCluster.TEST)

    val result = tableEnv.executeSql(
      """
        | INSERT INTO REDIS_SINK_DEMO
        | VALUES (1, 'test'), (2, 'test2'), (1, 'test1')
        |""".stripMargin)

    var status = result.getJobClient.get().getJobStatus.get()
    while (isNotStop(status)) {
      status = result.getJobClient.get().getJobStatus.get()
    }

    destroyContainerEnv()
  }

  val finishedStatusSet = Set(JobStatus.FINISHED, JobStatus.FAILED, JobStatus.CANCELED)

  def isNotStop(status: JobStatus): Boolean = {
    !finishedStatusSet.contains(status)
  }
}

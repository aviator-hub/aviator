package io.github.aviatorhub.aviator.example

import io.github.aviatorhub.aviator.app.AviatorSqlApp
import org.testcontainers.containers.{ClickHouseContainer, GenericContainer, MySQLContainer}
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.lifecycle.Startable
import org.testcontainers.utility.DockerImageName

object DemoEnv extends AviatorSqlApp() {

  val mysqlContainer: Startable =
    new MySQLContainer(DockerImageName.parse("mysql:5.7.34"))

  val redisContainer: Startable =
    new GenericContainer(DockerImageName.parse("redis:6.2.5"))
      .withExposedPorts(6379)

  val elasticsearchContainer = new ElasticsearchContainer()


  val clickHouseContainer =
    new ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server")
      .withTag("20.8.19.4"))

  def startEnv(): Unit = {
    mysqlContainer.start()
    redisContainer.start()
    elasticsearchContainer.start()
    clickHouseContainer.start()
  }

  def destoryEnv() : Unit = {
    mysqlContainer.stop()
    redisContainer.stop()
    elasticsearchContainer.stop()
    clickHouseContainer.stop()
  }

}

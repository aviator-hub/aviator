package io.github.aviatorhub.aviator.app.constant;

import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 */
public enum ContainerEnv {
  REDIS(new GenericContainer<>(DockerImageName.parse("redis:6.2.5"))
      .withExposedPorts(6379)),
  CLICKHOUSE(new ClickHouseContainer(
      DockerImageName.parse("yandex/clickhouse-server").withTag("20.8.19.4")));

  ContainerEnv(GenericContainer<?> container) {
    this.container = container;
  }

  private final GenericContainer<?> container;

  public GenericContainer<?> getContainer() {
    return container;
  }
}

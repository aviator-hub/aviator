package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.github.aviatorhub.aviator.connector.EnvMode;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * TODO support redis connection pool
 */
public class RedisConnectionProvider implements AutoCloseable {

  private ConnectorConf conf;
  private RedisClusterClient clusterClient;
  private RedisClient client;

  private StatefulRedisClusterConnection<String, String> sharedClusterConn;
  private StatefulRedisConnection<String, String> sharedConn;

  public RedisConnectionProvider(ConnectorConf conf) {
    this.conf = conf;
    String[] addresses = conf.getAddress().split(",");
    List<RedisURI> uriList = new LinkedList<>();
    for (String address : addresses) {
      String host = StringUtils.substringBefore(address, ":");
      int port = Integer.parseInt(StringUtils.substringAfter(address, ":"));

      Builder builder = RedisURI.builder()
          .withHost(host)
          .withPort(port);
      if (StringUtils.isNotBlank(conf.getPasswd())) {
        builder.withPassword(conf.getPasswd().toCharArray());
      }
      uriList.add(builder.build());
    }

    if (uriList.isEmpty()) {
      throw new RuntimeException();
    }

    if (EnvMode.CLUSTER.equals(conf.getEnvMode())) {
      clusterClient = RedisClusterClient.create(uriList);
    } else {
      client = RedisClient.create(uriList.get(0));
    }
  }

  public StatefulRedisClusterConnection<String, String> getClusterConnection() {
    if (sharedClusterConn == null) {
      synchronized (this) {
        if (sharedClusterConn == null) {
          sharedClusterConn = clusterClient.connect();
        }
      }
    }
    return sharedClusterConn;
  }

  public StatefulRedisConnection<String, String> getConnection() {
    if (sharedConn == null) {
      synchronized (this) {
        if (sharedConn == null) {
          sharedConn = client.connect();
        }
      }
    }
    return sharedConn;
  }

  @Override
  public void close() {
    if (clusterClient != null) {
      clusterClient.shutdown();
    }
  }
}

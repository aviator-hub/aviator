package io.github.aviatorhub.aviator.connector.redis;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import io.lettuce.core.RedisURI;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * TODO support redis connection pool
 */
public class RedisClient implements AutoCloseable {

  private final RedisClusterClient client;
  private static StatefulRedisClusterConnection<String, String> SHARED_CONN;

  public RedisClient(ConnectorConf conf) {
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

    client = RedisClusterClient.create(uriList);
  }

  public StatefulRedisClusterConnection<String, String> getConnection() {
    if (SHARED_CONN == null) {
      synchronized (this.getClass()) {
        if (SHARED_CONN == null) {
          SHARED_CONN = client.connect();
        }
      }
    }
    return SHARED_CONN;
  }

  @Override
  public void close() {
    if (client != null) {
      client.shutdown();
    }
  }
}

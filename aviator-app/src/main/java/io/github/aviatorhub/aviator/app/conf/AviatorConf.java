package io.github.aviatorhub.aviator.app.conf;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Aviator main configuration.
 *
 * @author jie mei
 */
@Getter
@Setter
@NoArgsConstructor
public class AviatorConf {

  private String jobName;

  private List<ConnectorConf> redisConns;
  private List<ConnectorConf> clickhouseConns;
  private List<ConnectorConf> mysqlConns;
  private List<ConnectorConf> elasticsearchConns;
}

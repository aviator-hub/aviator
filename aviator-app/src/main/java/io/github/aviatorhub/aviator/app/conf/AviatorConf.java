package io.github.aviatorhub.aviator.app.conf;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import java.util.List;

/**
 * Aviator main configuration.
 *
 * @author jie mei
 */
public class AviatorConf {

  private String jobName;

  private List<ConnectorConf> redisConns;
  private List<ConnectorConf> clickhouseConns;
  private List<ConnectorConf> mysqlConns;
  private List<ConnectorConf> elasticsearchConns;

  public AviatorConf() {
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public List<ConnectorConf> getRedisConns() {
    return redisConns;
  }

  public void setRedisConns(
      List<ConnectorConf> redisConns) {
    this.redisConns = redisConns;
  }

  public List<ConnectorConf> getClickhouseConns() {
    return clickhouseConns;
  }

  public void setClickhouseConns(
      List<ConnectorConf> clickhouseConns) {
    this.clickhouseConns = clickhouseConns;
  }

  public List<ConnectorConf> getMysqlConns() {
    return mysqlConns;
  }

  public void setMysqlConns(
      List<ConnectorConf> mysqlConns) {
    this.mysqlConns = mysqlConns;
  }

  public List<ConnectorConf> getElasticsearchConns() {
    return elasticsearchConns;
  }

  public void setElasticsearchConns(
      List<ConnectorConf> elasticsearchConns) {
    this.elasticsearchConns = elasticsearchConns;
  }
}

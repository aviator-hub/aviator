package io.github.aviatorhub.aviator.app.conf;

import io.github.aviatorhub.aviator.connector.ConnectorConf;
import java.util.List;

/**
 * Aviator main configuration.
 *
 * @author jie mei
 */
public class AviatorConf {

  private String projectName;
  private String jobName;

  private List<ConnectorConf> mysqlList;
  private List<ConnectorConf> redisList;
  private List<ConnectorConf> hbaseList;
  private List<ConnectorConf> elasticsearchList;
  private List<ConnectorConf> clickhouseList;

  public AviatorConf() {
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public List<ConnectorConf> getRedisList() {
    return redisList;
  }

  public void setRedisList(
      List<ConnectorConf> redisList) {
    this.redisList = redisList;
  }

  public List<ConnectorConf> getClickhouseList() {
    return clickhouseList;
  }

  public void setClickhouseList(
      List<ConnectorConf> clickhouseList) {
    this.clickhouseList = clickhouseList;
  }

  public List<ConnectorConf> getMysqlList() {
    return mysqlList;
  }

  public void setMysqlList(
      List<ConnectorConf> mysqlList) {
    this.mysqlList = mysqlList;
  }

  public List<ConnectorConf> getElasticsearchList() {
    return elasticsearchList;
  }

  public void setElasticsearchList(
      List<ConnectorConf> elasticsearchList) {
    this.elasticsearchList = elasticsearchList;
  }

  public List<ConnectorConf> getHbaseList() {
    return hbaseList;
  }

  public void setHbaseList(List<ConnectorConf> hbaseList) {
    this.hbaseList = hbaseList;
  }
}

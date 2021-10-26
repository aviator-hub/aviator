package io.github.aviatorhub.aviator.connector;

import io.github.aviatorhub.aviator.core.AviatorBufferConf;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

public class ConnectorConf implements Serializable {

  private String name;

  // common config
  private String address;
  private String database;
  private String table;
  private String user;
  private String passwd;
  private Integer parallel = 1;

  // lookup join config
  private Integer coreConnSize = 5;
  private Integer maxConnSize = 15;
  private Integer cacheSize = 1000;
  // time unit is second
  private Integer cacheTime = 300;

  // sink config
  private Boolean ordered = true;
  private Integer sinkRetryCnt= 3;
  private Integer sinkBatchSize = 5000;
  private Integer sinkFlushInterval = 5;
  private Boolean sinkBufferCompaction = false;

  private String keyPrefix = "";
  private Integer dataExpireSecond = 3600 * 24;

  // TODO run at production.
  private EnvMode envMode = EnvMode.SINGLETON ;

  private Map<String, String> props;

  public ConnectorConf() {
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void setPasswd(String passwd) {
    this.passwd = passwd;
  }

  public void setParallel(Integer parallel) {
    if (parallel != null && parallel > 0) {
      this.parallel = parallel;
    }  // else use default parallel 1
  }

  public void setCoreConnSize(Integer coreConnSize) {
    if (coreConnSize != null && coreConnSize > 0) {
      this.coreConnSize = coreConnSize;
    }
  }

  public void setMaxConnSize(Integer maxConnSize) {
    if (maxConnSize != null && maxConnSize > 0) {
      this.maxConnSize = maxConnSize;
    }
  }

  public void setCacheSize(Integer cacheSize) {
    if (cacheSize != null && cacheSize > 0) {
      this.cacheSize = cacheSize;
    } else {
      cacheSize = 0;
      // TODO warning the cache is disabled
    }
  }

  public void setCacheTime(Integer cacheTime) {
    if (cacheTime != null && cacheTime > 0) {
      this.cacheTime = cacheTime;
    } else {
      cacheTime = 0;
      // TODO warning the cache is disabled
    }
  }

  public void setSinkBatchSize(Integer sinkBatchSize) {
    if (sinkBatchSize != null && sinkBatchSize > 0) {
      this.sinkBatchSize = sinkBatchSize;
    } else {
      this.sinkBatchSize = 0;
    }
  }

  public void setSinkFlushInterval(Integer sinkFlushInterval) {
    if (sinkFlushInterval != null && sinkFlushInterval > 0) {
      this.sinkFlushInterval = sinkFlushInterval;
    } else {
      this.sinkFlushInterval = 0;
    }
  }

  public void setSinkRetryCnt(Integer sinkRetryCnt) {
    if (sinkRetryCnt != null && sinkRetryCnt >= 0) {
      this.sinkRetryCnt = sinkRetryCnt;
    }
  }

  public void setOrdered(Boolean ordered) {
    if (ordered != null) {
      this.ordered = ordered;
    } // else use default value true.
  }

  public void setKeyPrefix(String keyPrefix) {
    this.keyPrefix = StringUtils.defaultString(keyPrefix);
  }

  public void setDataExpireSecond(Integer dataExpireSecond) {
    this.dataExpireSecond = dataExpireSecond;
  }

  public void setSinkBufferCompaction(Boolean sinkBufferCompaction) {
    this.sinkBufferCompaction = sinkBufferCompaction;
  }

  public void setEnvMode(EnvMode envMode) {
    this.envMode = envMode;
  }

  public AviatorBufferConf getBufferConf() {
    AviatorBufferConf conf = new AviatorBufferConf();
    conf.setOrdered(this.ordered);
    conf.setParallel(this.parallel);
    conf.setRetryCnt(this.sinkRetryCnt);
    conf.setTimeoutSeconds(this.sinkFlushInterval);
    conf.setSize(this.sinkBatchSize);
    return conf;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public void setProp(String key, String value) {
    if (this.props == null) {
      props = new HashMap<>();
    }
    props.put(key, value);
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getAddress() {
    return address;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getUser() {
    return user;
  }

  public String getPasswd() {
    return passwd;
  }

  public Integer getParallel() {
    return parallel;
  }

  public Integer getCoreConnSize() {
    return coreConnSize;
  }

  public Integer getMaxConnSize() {
    return maxConnSize;
  }

  public Integer getCacheSize() {
    return cacheSize;
  }

  public Integer getCacheTime() {
    return cacheTime;
  }

  public Boolean getOrdered() {
    return ordered;
  }

  public Integer getSinkRetryCnt() {
    return sinkRetryCnt;
  }

  public Integer getSinkBatchSize() {
    return sinkBatchSize;
  }

  public Integer getSinkFlushInterval() {
    return sinkFlushInterval;
  }

  public Boolean getSinkBufferCompaction() {
    return sinkBufferCompaction;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }

  public Integer getDataExpireSecond() {
    return dataExpireSecond;
  }

  public EnvMode getEnvMode() {
    return envMode;
  }

  public Map<String, String> getProps() {
    return props;
  }

  @Override
  public String toString() {
    return "ConnectorConf{" +
        "name='" + name + '\'' +
        ", address='" + address + '\'' +
        ", database='" + database + '\'' +
        ", table='" + table + '\'' +
        ", user='" + user + '\'' +
        ", passwd='" + passwd + '\'' +
        ", parallel=" + parallel +
        ", coreConnSize=" + coreConnSize +
        ", maxConnSize=" + maxConnSize +
        ", cacheSize=" + cacheSize +
        ", cacheTime=" + cacheTime +
        ", ordered=" + ordered +
        ", sinkRetryCnt=" + sinkRetryCnt +
        ", sinkBatchSize=" + sinkBatchSize +
        ", sinkFlushInterval=" + sinkFlushInterval +
        ", sinkBufferCompaction=" + sinkBufferCompaction +
        ", keyPrefix='" + keyPrefix + '\'' +
        ", dataExpireSecond=" + dataExpireSecond +
        ", envMode=" + envMode +
        ", props=" + props +
        '}';
  }
}

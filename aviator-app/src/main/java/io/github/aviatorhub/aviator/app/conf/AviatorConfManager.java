package io.github.aviatorhub.aviator.app.conf;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import io.github.aviatorhub.aviator.app.annotation.JobDeclare;
import io.github.aviatorhub.aviator.app.annotation.PerfDeclare;
import io.github.aviatorhub.aviator.app.annotation.ResourceDeclare;
import io.github.aviatorhub.aviator.app.constant.ConnType;
import io.github.aviatorhub.aviator.connector.ConnectorConf;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class AviatorConfManager {

  private static final String DEFAULT_AVIATOR_CONF_PATH = "aviator.yml";
  private static final Map<ConnType, Map<String, ConnectorConf>> connMap = new HashMap<>();


  public static AviatorJobConf applyJobDeclare(Class<?> app) {
    AviatorJobConf aviatorJobConf = new AviatorJobConf();
    JobDeclare jobDeclare = app.getAnnotation(JobDeclare.class);
    PerfDeclare perfDeclare = app.getAnnotation(PerfDeclare.class);
    ResourceDeclare resourceDeclare = app.getAnnotation(ResourceDeclare.class);
    return aviatorJobConf;
  }

  public static void loadAviatorConf() throws FileNotFoundException, YamlException {
    loadAviatorConf(DEFAULT_AVIATOR_CONF_PATH);
  }

  public static void loadAviatorConf(String confPath) throws FileNotFoundException, YamlException {
    File confFile = new File(confPath);
    Optional<YamlReader> readerOpt = createConfReader(confPath, confFile);
    if (readerOpt.isPresent()) {
      AviatorConf conf = readerOpt.get().read(AviatorConf.class);
      prepareConnConf(conf.getRedisConns(), ConnType.REDIS);
      prepareConnConf(conf.getMysqlConns(), ConnType.MYSQL);
      prepareConnConf(conf.getClickhouseConns(), ConnType.CLICKHOUSE);
    }
  }

  private static Optional<YamlReader> createConfReader(String confPath, File confFile)
      throws FileNotFoundException {
    if (confFile.exists()) {
      return Optional.of(new YamlReader(new FileReader(confFile)));
    } else {
      InputStream resource = Thread.currentThread().getContextClassLoader()
          .getResourceAsStream(confPath);
      if (resource != null) {
        return Optional.of(new YamlReader(new InputStreamReader(resource)));
      }
    }
    return Optional.empty();
  }

  public static void prepareConnConf(List<ConnectorConf> confList, ConnType type) {
    Map<String, ConnectorConf> map = connMap.computeIfAbsent(type, key -> new HashMap<>());
    for (ConnectorConf conf : confList) {
      if (StringUtils.isBlank(conf.getName())) {
        log.warn("empty connection name with connection information: {}", conf.toString());
        continue;
      }
      if (map.containsKey(conf.getName())) {
        log.warn("duplicate {} connection named {}", type.name(), conf.getName());
        continue;
      }
      map.put(conf.getName(), conf);
    }
  }

  public static ConnectorConf getConnConf(String name, ConnType type) {
    Map<String, ConnectorConf> map = connMap.get(type);
    return map == null ? null : map.get(name);
  }

}

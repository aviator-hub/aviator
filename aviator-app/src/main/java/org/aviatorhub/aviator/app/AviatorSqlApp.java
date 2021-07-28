package org.aviatorhub.aviator.app;

import com.beust.jcommander.JCommander;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.aviatorhub.aviator.app.conf.AviatorJobConf;
import org.aviatorhub.aviator.app.constant.RunningMode;

@Slf4j
public abstract class AviatorSqlApp {

  private final AviatorJobConf jobConf = new AviatorJobConf();
  private TableEnvironment tableEnv;

  public void init(String[] args) {
    parseArgs(args);
    applyDefaultConf();
    buildTableEnv();
  }


  private void parseArgs(String[] args) {
    JCommander.newBuilder().addObject(new AviatorJobConf()).args(args).build();
  }

  private void applyDefaultConf() {
    if (jobConf.getRunningMode() == null) {
      jobConf.setRunningMode(RunningMode.TEST);
    }

    if (StringUtils.isBlank(jobConf.getJobName())) {
      jobConf.setJobName(this.getClass().getName());
    }

    if (jobConf.getCheckpointingMode() == null) {
      jobConf.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    }

    if (jobConf.getCheckpointInterval() == null) {
      jobConf.setCheckpointInterval(600L);
    }
  }

  public void buildTableEnv() {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    streamEnv.enableCheckpointing(jobConf.getCheckpointInterval() * 1000,
        jobConf.getCheckpointingMode());

    if (jobConf.getRunningMode().equals(RunningMode.PRODUCTION)) {
      streamEnv.setRestartStrategy(RestartStrategies.failureRateRestart(3,
          Time.of(10, TimeUnit.MINUTES),
          Time.of(1, TimeUnit.MINUTES)));

      // FIXME memory state backend shouldn't be used at production.
      streamEnv.setStateBackend(new HashMapStateBackend());
      streamEnv.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
      EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .useBlinkPlanner().inStreamingMode().build();
      this.tableEnv = StreamTableEnvironment.create(streamEnv, settings);
    } else {
      streamEnv.setStateBackend(new HashMapStateBackend());
      streamEnv.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
    }
  }
}

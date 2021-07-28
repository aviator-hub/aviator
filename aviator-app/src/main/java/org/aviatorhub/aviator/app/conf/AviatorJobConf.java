package org.aviatorhub.aviator.app.conf;

import com.beust.jcommander.Parameter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.aviatorhub.aviator.app.constant.CheckpointStateBackend;
import org.aviatorhub.aviator.app.constant.RunningMode;

@Getter
@Setter
@NoArgsConstructor
public class AviatorJobConf {

  // common conf
  @Parameter(names = {"-v", "-version"},
      description = "the version of this job, default is empty string")
  private String version;

  @Parameter(names = {"-n", "-name"},
      description = "the name of this job, default is class name")
  private String jobName;

  @Parameter(names = {"-rm", "-running-mode"},
      description = "different running mode may load different configuration, default is TEST")
  private RunningMode runningMode;

  // =============================================
  // == Checkpoint
  // =============================================
  @Parameter(names = {"-cm", "-checkpoint-mode"},
      description = "the checkpoint mode for this job, default is AT_LEAST_ONCE")
  private CheckpointingMode checkpointingMode;

  @Parameter(names = {"-increment-checkpoint"})
  private boolean incrementCheckpoint = false;

  @Parameter(names = {"-checkpoint-state-backend"})
  private CheckpointStateBackend checkpointStateBackend;

  @Parameter(names = {"-ci", "-checkpoint-interval"},
  description = "the checkpoint interval for this job, default is 10 minutes")
  private Long checkpointInterval;


  // source conf
  private AviatorSourceConf aviatorSourceConf;

  // TODO mem conf
  // TODO perf conf
}

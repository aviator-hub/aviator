package io.github.aviatorhub.aviator.app.conf;

import com.beust.jcommander.Parameter;
import io.github.aviatorhub.aviator.app.constant.CheckpointStateBackend;
import io.github.aviatorhub.aviator.app.constant.RunningMode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.streaming.api.CheckpointingMode;

@Getter
@Setter
@NoArgsConstructor
public class AviatorJobConf {

  // ===============================================
  // JOB DECLARE INFORMATION
  // ===============================================
  @Parameter(names = {"-n", "-name"},
      description = "the name of this job, default is class name")
  private String jobName;

  @Parameter(names = {"-v", "-version"},
      description = "the version of this job, default is empty string")
  private int version;

  @Parameter(names = {"-rt", "-realtime"})
  private boolean realtime;

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

  @Parameter(names = {"-cp", "-checkpoint-interval"},
      description = "the checkpoint hdfs path for this job")
  private String checkpointPath;


  // source conf
  private AviatorSourceConf aviatorSourceConf;

  // TODO mem conf

  // =============================================
  // == Performance Declare
  // =============================================
  private boolean twoPhaseAgg;
  private boolean miniBatch;
  private boolean distinctAggSplit;
}

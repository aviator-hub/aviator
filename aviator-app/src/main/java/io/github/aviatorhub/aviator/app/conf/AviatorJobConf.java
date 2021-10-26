package io.github.aviatorhub.aviator.app.conf;

import com.beust.jcommander.Parameter;
import io.github.aviatorhub.aviator.app.constant.CheckpointStateBackend;
import io.github.aviatorhub.aviator.app.constant.RunningMode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.streaming.api.CheckpointingMode;

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
  private RunningMode runningMode = RunningMode.TEST;

  // =============================================
  // == Checkpoint
  // =============================================
  @Parameter(names = {"-cm", "-checkpoint-mode"},
      description = "the checkpoint mode for this job, default is AT_LEAST_ONCE")
  private CheckpointingMode checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;

  @Parameter(names = {"-increment-checkpoint"})
  private boolean incrementCheckpoint = false;

  @Parameter(names = {"-checkpoint-state-backend"})
  private CheckpointStateBackend checkpointStateBackend;

  @Parameter(names = {"-ci", "-checkpoint-interval"},
      description = "the checkpoint interval for this job, default is 10 minutes")
  private Long checkpointInterval = 600000L;

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

  public AviatorJobConf() {
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public boolean isRealtime() {
    return realtime;
  }

  public void setRealtime(boolean realtime) {
    this.realtime = realtime;
  }

  public RunningMode getRunningMode() {
    return runningMode;
  }

  public void setRunningMode(RunningMode runningMode) {
    this.runningMode = runningMode;
  }

  public CheckpointingMode getCheckpointingMode() {
    return checkpointingMode;
  }

  public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
    this.checkpointingMode = checkpointingMode;
  }

  public boolean isIncrementCheckpoint() {
    return incrementCheckpoint;
  }

  public void setIncrementCheckpoint(boolean incrementCheckpoint) {
    this.incrementCheckpoint = incrementCheckpoint;
  }

  public CheckpointStateBackend getCheckpointStateBackend() {
    return checkpointStateBackend;
  }

  public void setCheckpointStateBackend(
      CheckpointStateBackend checkpointStateBackend) {
    this.checkpointStateBackend = checkpointStateBackend;
  }

  public Long getCheckpointInterval() {
    return checkpointInterval;
  }

  public void setCheckpointInterval(Long checkpointInterval) {
    this.checkpointInterval = checkpointInterval;
  }

  public String getCheckpointPath() {
    return checkpointPath;
  }

  public void setCheckpointPath(String checkpointPath) {
    this.checkpointPath = checkpointPath;
  }

  public AviatorSourceConf getAviatorSourceConf() {
    return aviatorSourceConf;
  }

  public void setAviatorSourceConf(
      AviatorSourceConf aviatorSourceConf) {
    this.aviatorSourceConf = aviatorSourceConf;
  }

  public boolean isTwoPhaseAgg() {
    return twoPhaseAgg;
  }

  public void setTwoPhaseAgg(boolean twoPhaseAgg) {
    this.twoPhaseAgg = twoPhaseAgg;
  }

  public boolean isMiniBatch() {
    return miniBatch;
  }

  public void setMiniBatch(boolean miniBatch) {
    this.miniBatch = miniBatch;
  }

  public boolean isDistinctAggSplit() {
    return distinctAggSplit;
  }

  public void setDistinctAggSplit(boolean distinctAggSplit) {
    this.distinctAggSplit = distinctAggSplit;
  }
}

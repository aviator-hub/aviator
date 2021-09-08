package io.github.aviatorhub.aviator.app.constant;

public enum ConsumeMode {
  EARLIEST("earliest-offset"),
  LATEST("latest-offset"),
  GROUP_OFFSETS("group-offsets"),
  TIMESTAMP("timestamp");

  private String opValue;

  ConsumeMode(String opValue) {
    this.opValue = opValue;
  }
}

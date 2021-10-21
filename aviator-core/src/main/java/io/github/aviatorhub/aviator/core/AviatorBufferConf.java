package io.github.aviatorhub.aviator.core;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AviatorBufferConf implements Serializable, Cloneable {

  private int size = 5000;
  private int parallel = 1;
  private int retryCnt = 3;
  private int timeoutSeconds = 5;
  private boolean ordered = true;

  @Override
  protected AviatorBufferConf clone() throws CloneNotSupportedException {
    AviatorBufferConf copy = new AviatorBufferConf();
    copy.setSize(this.getSize());
    copy.setParallel(this.getParallel());
    copy.setRetryCnt(this.getRetryCnt());
    copy.setTimeoutSeconds(this.getTimeoutSeconds());
    copy.setOrdered(this.isOrdered());
    return copy;
  }
}

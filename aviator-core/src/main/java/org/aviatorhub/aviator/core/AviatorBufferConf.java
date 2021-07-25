package org.aviatorhub.aviator.core;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AviatorBufferConf implements Serializable {

  private int size = 5000;
  private int parallel = 1;
  private int retryCnt = 3;
  private int timeoutSeconds = 5;
  private boolean ordered = true;
}

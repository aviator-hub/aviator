package io.github.aviatorhub.aviator.core;

import java.util.Random;

public class AviatorMockPartitioner implements AviatorPartitioner<Long> {

  private AviatorBufferConf conf;

  public AviatorMockPartitioner(AviatorBufferConf conf) {
    this.conf = conf;
  }

  @Override
  public int calcPartition(Long aLong) {
    Random random = new Random();
    return random.nextInt(conf.getParallel());
  }
}

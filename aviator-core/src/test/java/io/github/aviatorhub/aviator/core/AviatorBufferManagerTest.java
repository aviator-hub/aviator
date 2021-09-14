package io.github.aviatorhub.aviator.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.types.logical.SmallIntType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.utility.ImageNameSubstitutor;

public class AviatorBufferManagerTest {

  private AviatorBufferConf conf;

  @Before
  public void before() {
    conf = new AviatorBufferConf();
    conf.setOrdered(false);
    conf.setParallel(4);
    conf.setRetryCnt(0);
    conf.setSize(10);
    conf.setTimeoutSeconds(1);
  }

  @Test
  public void testBase() throws InterruptedException {
    final long testCnt = 1_000_000;
    final AviatorMockFlusher flusher = new AviatorMockFlusher(0);
    final AviatorBufferManager<Long> manger = new AviatorBufferManager(
        conf,
        Long.class,
        new AviatorMockPartitioner(conf),
        flusher);
    manger.init();

    CountDownLatch latch = new CountDownLatch(conf.getParallel());
    for (int i = 0; i < conf.getParallel(); i++) {
      Thread thread = new Thread(() -> {
        for (int j = 0; j < testCnt; j++) {
          try {
            manger.add(1l);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        latch.countDown();
      });
      thread.start();
    }

    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
      try {
        manger.checkpoint();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 0, 10, TimeUnit.MILLISECONDS);

    manger.startTimeoutTrigger();
    latch.await();
    Assert.assertEquals(conf.getParallel() * testCnt, flusher.getValue());
  }
}

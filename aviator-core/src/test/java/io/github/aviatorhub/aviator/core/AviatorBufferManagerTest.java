package io.github.aviatorhub.aviator.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class AviatorBufferManagerTest {


  private AviatorBufferConf buildBaseConf() {
    AviatorBufferConf conf = new AviatorBufferConf();
    conf.setOrdered(true);
    conf.setParallel(4);
    conf.setRetryCnt(0);
    conf.setSize(10);
    conf.setTimeoutSeconds(1);
    return conf;
  }

  @Test
  public void testBase() throws Exception {
    AviatorBufferConf conf = buildBaseConf();
    baseSinkDataTest(conf);
    AviatorBufferConf copy = conf.clone();
    copy.setOrdered(false);
    baseSinkDataTest(copy);
  }

  private void baseSinkDataTest(AviatorBufferConf bufferConf) throws Exception {
    final long testCnt = 1_000_000;
    final AviatorMockFlusher flusher = new AviatorMockFlusher(0);
    final AviatorBufferManager<Long> manger = new AviatorBufferManager(
        bufferConf,
        Long.class,
        new AviatorMockPartitioner(bufferConf),
        flusher);
    manger.init();

    CountDownLatch latch = new CountDownLatch(bufferConf.getParallel());
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    for (int i = 0; i < bufferConf.getParallel(); i++) {
      executorService.submit(() -> {
        for (int j = 0; j < testCnt; j++) {
          try {
            manger.add(1L);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        latch.countDown();
      });
    }

    ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor();
    timeoutScheduler.scheduleAtFixedRate(() -> {
      try {
        manger.checkpoint();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 0, 10, TimeUnit.MILLISECONDS);

    manger.startTimeoutTrigger();
    latch.await();
    manger.checkpoint();
    assertEquals(bufferConf.getParallel() * testCnt, flusher.getValue());
    timeoutScheduler.shutdownNow();
    executorService.shutdownNow();
  }

}

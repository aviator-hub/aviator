package io.github.aviatorhub.aviator.core;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.CollectionUtil;

/**
 * AviatorBufferManager
 *
 * @param <V>
 * @author meijie
 */
@Slf4j
public class AviatorBufferManager<V> {

  private final AviatorBufferConf conf;
  private final Class<V> valueClass;
  private final AviatorBuffer<V>[] buffers;
  private final AtomicBoolean[] flushingArray;
  private final AviatorPartitioner<V> partitioner;
  private final AviatorFlusher<V> flusher;
  private final AtomicReference<Exception> exceptionHolder = new AtomicReference<>(null);

  private final ReadWriteLock checkpointLock = new ReentrantReadWriteLock();

  public AviatorBufferManager(AviatorBufferConf conf,
      Class<V> valueClass,
      AviatorPartitioner<V> partitioner,
      AviatorFlusher<V> flusher) {

    checkNotNull(conf);
    checkArgument(conf.getSize() > 0);
    checkArgument(conf.getParallel() > 0);
    checkArgument(conf.getTimeoutSeconds() > 0);

    checkNotNull(valueClass);
    checkNotNull(partitioner);
    checkNotNull(flusher);

    this.conf = conf;
    this.valueClass = valueClass;
    this.partitioner = partitioner;
    this.flusher = flusher;
    buffers = new AviatorBuffer[conf.getParallel()];
    flushingArray = new AtomicBoolean[conf.getParallel()];
  }

  public void init() {
    for (int i = 0; i < conf.getParallel(); i++) {
      buffers[i] = new AviatorBuffer(valueClass, conf.getSize());
      flushingArray[i] = new AtomicBoolean(false);
    }
  }

  public void add(V v) throws Exception {
    checkException();
    final int partition = partitioner.calcPartition(v);
    AviatorBuffer<V> buffer = buffers[partition];
    V[] values = buffer.addValue(v);
    if (values != null && values.length > 0) {
      checkpointLock.readLock().lock();
      try {
        if (conf.isOrdered()) {
          flushOrdered(values, flushingArray[partition]);
        } else {
          flush(values);
        }
      } finally {
        checkpointLock.readLock().unlock();
      }
    }
  }

  public void startTimeoutTrigger() {
    Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("aviator-buffer-manager-timeout-" + "-%d")
                .setUncaughtExceptionHandler((thread, throwable) ->
                    log.error(thread.getName() + " execute failure with uncaught exception", throwable))
                .build())
        .scheduleAtFixedRate(this::timeout,
            conf.getTimeoutSeconds(),
            conf.getTimeoutSeconds(),
            TimeUnit.SECONDS);
  }

  public void checkpoint() throws Exception {
    checkpointLock.writeLock().lock();
    try {
      List<CompletableFuture<Void>> taskList = new LinkedList<>();
      for (int i = 0; i < buffers.length; i++) {
        V[] values = buffers[i].onDrain();
        if (values != null && values.length > 0) {
          if (conf.isOrdered()) {
            taskList.add(flushOrdered(values, flushingArray[i]));
          } else {
            taskList.add(flush(values));
          }
        }
      }
      if (CollectionUtils.isNotEmpty(taskList)) {
        CompletableFuture.allOf(taskList.toArray(new CompletableFuture[0])).join();
      }
    } finally {
      checkpointLock.writeLock().unlock();
    }
    checkException();
  }

  private void timeout() {
    checkpointLock.readLock().lock();
    try {
      List<CompletableFuture<Void>> taskList = new LinkedList<>();
      for (int i = 0; i < buffers.length; i++) {
        AviatorBuffer<V> buffer = buffers[i];
        if (System.currentTimeMillis() - buffer.getLastFlushTimestamp()
            > conf.getTimeoutSeconds() * 1000L) {
          V[] values = buffer.onDrain();
          if (values != null && values.length > 0) {
            if (conf.isOrdered()) {
              taskList.add(flushOrdered(values, flushingArray[i]));
            } else {
              taskList.add(flush(values));
            }
          }
        }
      }
      if (CollectionUtils.isNotEmpty(taskList)) {
        CompletableFuture.allOf(taskList.toArray(new CompletableFuture[0])).join();
      }
    } finally {
      checkpointLock.readLock().unlock();
    }
  }

  private void checkException() throws Exception {
    if (exceptionHolder.get() != null) {
      throw exceptionHolder.get();
    }  // do nothing

  }

  private CompletableFuture<Void> flushOrdered(V[] values, AtomicBoolean flushing) {
    while (!flushing.compareAndSet(false, true)) {
      LockSupport.parkNanos(10000);
    }
    return CompletableFuture.runAsync(() -> {
      try {
        flusher.onFlush(values);
      } catch (Exception e) {
        exceptionPropagate(e);
        throw new RuntimeException(e);
      } finally {
        flushing.set(false);
      }
    });
  }

  private CompletableFuture<Void> flush(V[] values) {
    return CompletableFuture.runAsync(() -> {
      try {
        flusher.onFlush(values);
      } catch (Exception e) {
        exceptionPropagate(e);
        throw new RuntimeException(e);
      }
    });
  }

  private void exceptionPropagate(Exception e) {
    while (true) {
      if (exceptionHolder.get() != null
          || exceptionHolder.compareAndSet(null, e)) {
        break;
      }
    }
  }


}

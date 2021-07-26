package org.aviatorhub.aviator.core;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
      }finally {
        checkpointLock.readLock().unlock();
      }
    }
  }

  public void startTimeoutTrigger() {

  }

  public void checkpoint() throws Exception {
    checkpointLock.writeLock().lock();
    try {
      for (int i = 0; i < buffers.length; i++) {
        V[] values = buffers[i].onDrain();
        if (values != null && values.length > 0) {
          flush(values);
        }
      }
    } finally {
      checkpointLock.writeLock().unlock();
    }
    checkException();
  }

  private void timeout() {
    checkpointLock.readLock().lock();
    try {
      for (int i = 0; i < buffers.length; i++) {
        AviatorBuffer<V> buffer = buffers[i];
        if (System.currentTimeMillis() - buffer.getLastFlushTimestamp()
            > conf.getTimeoutSeconds() * 1000L) {
          V[] values = buffer.onDrain();
          if (values != null && values.length > 0) {
            if (conf.isOrdered()) {
              flushOrdered(values, flushingArray[i]);
            } else {
              flush(values);
            }
          }
        }
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

  private void flushOrdered(V[] values, AtomicBoolean flushing) {
    while (!flushing.compareAndSet(false, true)) {
      LockSupport.parkNanos(10000);
    }
    CompletableFuture.runAsync(() -> {
      try {
        flusher.onFlush(values, flushing);
      } catch (Exception e) {
        exceptionPropagate(e);
        throw new RuntimeException(e);
      }
    });
  }

  private void flush(V[] values) {
    CompletableFuture.runAsync(() -> {
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

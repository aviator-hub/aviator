package org.aviatorhub.aviator.core;

import java.util.concurrent.atomic.AtomicBoolean;

public interface AviatorFlusher<V> {

  void onFlush(V[] values) throws Exception;

  void onFlush(V[] values, AtomicBoolean flushing) throws Exception;
}

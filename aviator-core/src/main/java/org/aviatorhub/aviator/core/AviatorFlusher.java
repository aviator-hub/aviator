package org.aviatorhub.aviator.core;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public interface AviatorFlusher<V extends Serializable> {

  void onFlush(V[] values) throws Exception;

  void onFlush(V[] values, AtomicBoolean flushing) throws Exception;
}

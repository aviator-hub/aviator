package io.github.aviatorhub.aviator.core;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;

public class AviatorBuffer<V> implements Serializable{

  private final V[] values;
  private int index = 0;
  private final int size;
  private volatile long lastFlushTimestamp = System.currentTimeMillis();

  @SuppressWarnings("unchecked")
  public AviatorBuffer(Class<V> valueClass, int size) {
    this.size = size;
    values = (V[]) Array.newInstance(valueClass, size);
  }

  public synchronized V[] addValue(V v) {
    V[] copy = null;
    if (index < size) {
      values[index] = v;
      index++;
    } else {
      copy = Arrays.copyOf(values, index);
      values[0] = v;
      index = 1;
      lastFlushTimestamp = System.currentTimeMillis();
    }
    return copy;
  }

  public synchronized V[] onDrain() {
    V[] copy = null;
    if (index > 0 ){
      copy = Arrays.copyOf(values, index);
      index = 0;
      lastFlushTimestamp = System.currentTimeMillis();
    }
    return copy;
  }

  public long getLastFlushTimestamp() {
    return lastFlushTimestamp;
  }
}

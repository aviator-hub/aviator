package io.github.aviatorhub.aviator.core;

public interface AviatorPartitioner<V> {

  int calcPartition(V v);
}

package org.aviatorhub.aviator.core;

import java.io.Serializable;

public interface AviatorPartitioner<V extends Serializable> {

  int calcPartition(V v);
}

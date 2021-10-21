package io.github.aviatorhub.aviator.core;

import org.jetbrains.annotations.NotNull;

public interface AviatorFlusher<V> {

  void onFlush(@NotNull V[] values) throws Exception;
}

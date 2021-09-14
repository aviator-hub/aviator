package io.github.aviatorhub.aviator.core;

import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;

public interface AviatorFlusher<V> {

  void onFlush(@NotNull V[] values) throws Exception;

  void onFlush(@NotNull V[] values, @NotNull AtomicBoolean flushing) throws Exception;
}

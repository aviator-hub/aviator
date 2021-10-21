package io.github.aviatorhub.aviator.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AviatorBufferTest {

  @Test
  public void baseTest() {
    AviatorBuffer<Integer> buffer = new AviatorBuffer<>(Integer.class, 500);
    for (int i = 0; i < 500; i++) {
      buffer.addValue(1);
    }
    Integer[] data = buffer.addValue(1);
    assertEquals(500, data.length);
    for (int i = 0; i < 499; i++) {
      buffer.addValue(1);
    }
    data = buffer.addValue(1);
    assertEquals(500, data.length);
    for (int i = 0; i < 499; i++) {
      buffer.addValue(1);
    }
    assertEquals(500, buffer.onDrain().length);
  }
}

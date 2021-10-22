package io.github.aviatorhub.aviator.app.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface PerfDeclare {

  boolean twoPhaseAgg() default false;

  boolean miniBatch() default false;

  boolean distinctAggSplit() default false;
}

package io.github.aviatorhub.aviator.app.annotation;

import io.github.aviatorhub.aviator.app.constant.RunningMode;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A annotation be used to decorate flink job class
 *
 * @author jie mei
 * @since 2021/10/22
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface JobDeclare {

  /**
   * define the job name of given job. default value is fully-qualified class name
   *
   * @return the job name;
   */
  String value() default "";

  /**
   * define the version of given job. default version is 0
   *
   * @return the version
   */
  int version() default 0;

  /**
   * define the given job is realtime job or not. default the job is realtime job.
   *
   * @return is realtime job or not.
   */
  boolean realtime() default true;

  /**
   * define the environment this job should run at.
   *
   * @return the given environment.
   */
  RunningMode runningMode() default RunningMode.TEST;

}

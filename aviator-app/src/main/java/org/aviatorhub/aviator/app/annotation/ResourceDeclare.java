package org.aviatorhub.aviator.app.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ResourceDeclare {

  int totalParallel() default 1;

  /**
   * declare slot to be used by this job.
   */
  int slotPerTaskManager() default 1;

  /**
   * declare memory resources to be used by this job. the unit is mb
   */
  int taskManageMemory() default 1024;

  /**
   * declare memory resources to be used by job
   */
  int jobMangeMemory() default 1024;
}

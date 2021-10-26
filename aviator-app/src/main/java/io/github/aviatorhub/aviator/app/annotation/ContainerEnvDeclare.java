package io.github.aviatorhub.aviator.app.annotation;

import io.github.aviatorhub.aviator.app.constant.ContainerEnv;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declare the connector container needed for running job.
 *
 * @author jie mei
 * @since 2021/10/26
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface ContainerEnvDeclare {

  ContainerEnv[] value() default {};
}

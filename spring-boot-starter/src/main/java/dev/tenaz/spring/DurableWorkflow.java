package dev.tenaz.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link dev.tenaz.api.Workflow} for registration with the engine under the given type.
 * A class with this annotation in the application's packages becomes a Spring bean without
 * further annotations. It is an ordinary singleton, so it can have its collaborators injected;
 * what it must not have is state of its own, because one instance runs every execution.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DurableWorkflow {

    /** The workflow type: the name clients start it by, and what is recorded in the journal. */
    String value();
}

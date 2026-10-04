package dev.tenaz.api;

/**
 * A durable workflow: plain Java whose progress survives the death of the process running it.
 *
 * <p>The engine re-runs {@link #run} from the top every time the workflow has something new to
 * react to, feeding it the results recorded so far. That only works if the code is deterministic,
 * so everything that touches the outside world, reads a clock or draws randomness must go
 * through the {@link WorkflowContext}.
 */
@FunctionalInterface
public interface Workflow<I, O> {

    O run(WorkflowContext ctx, I input) throws Exception;
}

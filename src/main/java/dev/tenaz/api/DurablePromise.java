package dev.tenaz.api;

/** The eventual result of a step, timer or signal. */
public interface DurablePromise<T> {

    /** Returns the result, suspending the workflow until it is available. */
    T get();
}

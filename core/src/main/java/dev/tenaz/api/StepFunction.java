package dev.tenaz.api;

@FunctionalInterface
public interface StepFunction<T> {

    T apply(StepContext step) throws Exception;
}

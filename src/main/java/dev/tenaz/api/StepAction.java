package dev.tenaz.api;

@FunctionalInterface
public interface StepAction {

    void run(StepContext step) throws Exception;
}

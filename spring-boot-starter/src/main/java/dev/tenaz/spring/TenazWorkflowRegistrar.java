package dev.tenaz.spring;

import dev.tenaz.api.Workflow;
import dev.tenaz.engine.TenazEngine;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.ResolvableType;

/**
 * Registers the {@link DurableWorkflow} beans with the engine once every singleton exists, and
 * starts the engine's workers when the application starts. The workflows are looked up late, and
 * not injected into the engine, so that a workflow bean is free to depend on the engine.
 */
class TenazWorkflowRegistrar implements SmartInitializingSingleton, SmartLifecycle {

    private final TenazEngine engine;
    private final TenazProperties properties;
    private final ListableBeanFactory beans;
    private volatile boolean running;

    TenazWorkflowRegistrar(TenazEngine engine, TenazProperties properties, ListableBeanFactory beans) {
        this.engine = engine;
        this.properties = properties;
        this.beans = beans;
    }

    @Override
    public void afterSingletonsInstantiated() {
        beans.getBeansWithAnnotation(DurableWorkflow.class).forEach((name, bean) -> {
            String type = beans.findAnnotationOnBean(name, DurableWorkflow.class).value();
            if (!(bean instanceof Workflow<?, ?> workflow)) {
                throw new IllegalStateException("bean '" + name + "' is annotated with @DurableWorkflow(\""
                        + type + "\") but does not implement " + Workflow.class.getName());
            }
            register(name, type, workflow);
        });
    }

    @SuppressWarnings("unchecked")
    private <I, O> void register(String beanName, String type, Workflow<I, O> workflow) {
        ResolvableType declared = ResolvableType.forClass(AopUtils.getTargetClass(workflow)).as(Workflow.class);
        Class<?> input = declared.getGeneric(0).resolve();
        Class<?> output = declared.getGeneric(1).resolve();
        if (input == null || output == null) {
            throw new IllegalStateException("cannot tell the input and output types of workflow bean '" + beanName
                    + "': declare them, as in 'implements Workflow<Order, Receipt>'");
        }
        engine.register(type, (Class<I>) input, (Class<O>) output, workflow);
    }

    @Override
    public void start() {
        if (properties.isWorkersEnabled()) {
            engine.startWorkers();
        }
        running = true;
    }

    /** The engine is closed as a bean, after everything that might still be using it. */
    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}

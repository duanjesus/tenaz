package dev.tenaz.engine;

import dev.tenaz.api.Workflow;

record WorkflowDefinition<I, O>(String type, Class<I> inputType, Class<O> outputType, Workflow<I, O> workflow) {}

package dev.tenaz.spring;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.filter.AnnotationTypeFilter;

/**
 * Turns the {@link DurableWorkflow} classes in the application's packages into beans. The
 * annotation is deliberately not a Spring stereotype: its value is the workflow type, and a
 * stereotype's value would be taken for the bean's name.
 */
class DurableWorkflowScanner implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        if (!(registry instanceof BeanFactory beanFactory) || !AutoConfigurationPackages.has(beanFactory)) {
            return;
        }
        // Classes the application already declared as beans are recognized and left alone.
        ClassPathBeanDefinitionScanner scanner = new ClassPathBeanDefinitionScanner(registry, false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(DurableWorkflow.class));
        scanner.scan(AutoConfigurationPackages.get(beanFactory).toArray(String[]::new));
    }
}

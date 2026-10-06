package dev.tenaz.example;

import dev.tenaz.api.WorkflowCancelledException;
import dev.tenaz.api.WorkflowFailedException;
import dev.tenaz.api.WorkflowHandle;
import dev.tenaz.engine.TenazEngine;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    public record OrderStatus(String id, String status, String detail) {}

    private final TenazEngine engine;

    public OrderController(TenazEngine engine) {
        this.engine = engine;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderStatus place(@RequestBody Order order) {
        String id = UUID.randomUUID().toString();
        engine.start("order", id, order);
        return new OrderStatus(id, "RUNNING", null);
    }

    @PostMapping("/{id}/approval")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void approve(@PathVariable String id, @RequestParam String by) {
        order(id).signal("approve", by);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void cancel(@PathVariable String id, @RequestParam(defaultValue = "cancelled by customer") String reason) {
        order(id).cancel(reason);
    }

    @GetMapping("/{id}")
    public OrderStatus status(@PathVariable String id) throws InterruptedException {
        WorkflowHandle<String> order = order(id);
        if (!order.isDone()) {
            return new OrderStatus(id, "RUNNING", null);
        }
        try {
            return new OrderStatus(id, "COMPLETED", order.result(Duration.ofSeconds(1)));
        } catch (WorkflowCancelledException e) {
            return new OrderStatus(id, "CANCELLED", e.reason());
        } catch (WorkflowFailedException e) {
            return new OrderStatus(id, "FAILED", e.getMessage());
        } catch (TimeoutException e) {
            return new OrderStatus(id, "RUNNING", null);
        }
    }

    private WorkflowHandle<String> order(String id) {
        return engine.handle(id, String.class);
    }

    /** The engine reports an id it has never seen as an illegal argument. */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public String unknownOrder(IllegalArgumentException e) {
        return e.getMessage();
    }
}

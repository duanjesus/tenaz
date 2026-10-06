package dev.tenaz.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

/**
 * Stands in for a payment provider. Like a real one, it takes an idempotency key: a request
 * repeated with the same key returns the first answer instead of charging again. That is what
 * turns the engine's at-least-once steps into exactly-once payments.
 */
@Service
public class PaymentGateway {

    private final Map<String, String> charges = new ConcurrentHashMap<>();
    private final Map<String, String> refunds = new ConcurrentHashMap<>();

    public String charge(long amountCents, String idempotencyKey) {
        return charges.computeIfAbsent(idempotencyKey, key -> "ch_" + (charges.size() + 1));
    }

    public void refund(String chargeId, String idempotencyKey) {
        refunds.putIfAbsent(idempotencyKey, chargeId);
    }

    public int charges() {
        return charges.size();
    }

    public int refunds() {
        return refunds.size();
    }
}

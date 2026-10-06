package dev.tenaz.example;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;

/** Stands in for a shipping system; deduplicates on the idempotency key like the payment gateway. */
@Service
public class Warehouse {

    private final Map<String, String> shipments = new ConcurrentHashMap<>();

    public String ship(String item, String idempotencyKey) {
        return shipments.computeIfAbsent(idempotencyKey, key -> "TRK" + (1000 + shipments.size()));
    }

    public int shipments() {
        return shipments.size();
    }
}

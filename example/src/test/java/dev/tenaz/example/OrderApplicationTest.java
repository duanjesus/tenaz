package dev.tenaz.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.tenaz.example.OrderController.OrderStatus;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {"tenaz.poll-interval=5ms", "orders.approval-window=1h"})
@AutoConfigureMockMvc
class OrderApplicationTest {

    @Autowired
    private MockMvc http;

    @Autowired
    private ObjectMapper json;

    @Autowired
    private PaymentGateway payments;

    @Autowired
    private Warehouse warehouse;

    private String place(String item) throws Exception {
        String body = http.perform(post("/orders").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(new Order(item, 4_990))))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        OrderStatus placed = json.readValue(body, OrderStatus.class);
        assertThat(placed.status()).isEqualTo("RUNNING");
        return placed.id();
    }

    private OrderStatus awaitEnd(String id) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(10));
        while (true) {
            String body = http.perform(get("/orders/{id}", id)).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            OrderStatus current = json.readValue(body, OrderStatus.class);
            if (!current.status().equals("RUNNING") || Instant.now().isAfter(deadline)) {
                return current;
            }
            Thread.sleep(20);
        }
    }

    @Test
    void anApprovedOrderIsChargedOnceAndShipped() throws Exception {
        int chargesBefore = payments.charges();
        int shipmentsBefore = warehouse.shipments();
        String id = place("keyboard");

        http.perform(post("/orders/{id}/approval", id).param("by", "ana")).andExpect(status().isAccepted());

        OrderStatus ended = awaitEnd(id);
        assertThat(ended.status()).isEqualTo("COMPLETED");
        assertThat(ended.detail()).startsWith("SHIPPED TRK").endsWith("approved by ana");
        assertThat(payments.charges() - chargesBefore).isEqualTo(1);
        assertThat(warehouse.shipments() - shipmentsBefore).isEqualTo(1);
    }

    @Test
    void aCancelledOrderIsRefundedAndNeverShipped() throws Exception {
        int refundsBefore = payments.refunds();
        int shipmentsBefore = warehouse.shipments();
        String id = place("monitor");

        http.perform(delete("/orders/{id}", id).param("reason", "found it cheaper"))
                .andExpect(status().isAccepted());

        OrderStatus ended = awaitEnd(id);
        assertThat(ended.status()).isEqualTo("CANCELLED");
        assertThat(ended.detail()).isEqualTo("found it cheaper");
        assertThat(payments.refunds() - refundsBefore).isEqualTo(1);
        assertThat(warehouse.shipments() - shipmentsBefore).isZero();
    }

    @Test
    void anUnknownOrderIsNotFound() throws Exception {
        http.perform(get("/orders/{id}", "no-such-order")).andExpect(status().isNotFound());
    }
}

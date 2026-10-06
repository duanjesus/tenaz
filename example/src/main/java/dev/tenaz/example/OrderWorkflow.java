package dev.tenaz.example;

import dev.tenaz.api.DurablePromise;
import dev.tenaz.api.Workflow;
import dev.tenaz.api.WorkflowCancelledException;
import dev.tenaz.api.WorkflowContext;
import dev.tenaz.spring.DurableWorkflow;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;

/**
 * An order: charge the customer, wait for someone to approve it, ship it. The wait may last days
 * and the service may be redeployed many times meanwhile; the order carries on where it was.
 * If nobody approves in time, or the order is cancelled, the customer gets the money back.
 */
@DurableWorkflow("order")
public class OrderWorkflow implements Workflow<Order, String> {

    private final PaymentGateway payments;
    private final Warehouse warehouse;
    private final Duration approvalWindow;

    public OrderWorkflow(PaymentGateway payments, Warehouse warehouse,
                         @Value("${orders.approval-window}") Duration approvalWindow) {
        this.payments = payments;
        this.warehouse = warehouse;
        this.approvalWindow = approvalWindow;
    }

    @Override
    public String run(WorkflowContext ctx, Order order) {
        String chargeId = ctx.step("charge", String.class,
                step -> payments.charge(order.amountCents(), step.idempotencyKey()));
        try {
            DurablePromise<String> approval = ctx.signal("approve", String.class);
            DurablePromise<Void> deadline = ctx.timer(approvalWindow);
            if (ctx.anyOf(approval, deadline) == deadline) {
                ctx.run("refund", step -> payments.refund(chargeId, step.idempotencyKey()));
                return "EXPIRED";
            }
            String tracking = ctx.step("ship", String.class,
                    step -> warehouse.ship(order.item(), step.idempotencyKey()));
            return "SHIPPED " + tracking + ", approved by " + approval.get();
        } catch (WorkflowCancelledException e) {
            ctx.run("refund", step -> payments.refund(chargeId, step.idempotencyKey()));
            throw e;
        }
    }
}

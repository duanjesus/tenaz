package dev.tenaz.spring;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.tenaz.journal.Event;
import dev.tenaz.journal.Journal.WorkflowStatus;
import dev.tenaz.journal.JournalBrowser;
import dev.tenaz.journal.JournalBrowser.Filter;
import dev.tenaz.journal.JournalBrowser.RecordedEvent;
import dev.tenaz.journal.JournalBrowser.WorkflowSummary;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * A read-only window onto the journal: one page, and the two queries it makes. It changes
 * nothing and has no access control of its own, which is why it is off unless asked for.
 */
@RestController
@RequestMapping("/tenaz")
public class TenazViewerController {

    private static final int MAX_LISTED = 500;

    public record Listing(Map<WorkflowStatus, Long> counts, List<WorkflowSummary> workflows) {}

    public record EventView(long seq, Instant at, String type, Map<String, Object> data) {}

    public record Detail(WorkflowSummary workflow, List<EventView> events) {}

    private final JournalBrowser journal;
    private final String page;
    private final ObjectMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    public TenazViewerController(JournalBrowser journal) {
        this.journal = journal;
        try (InputStream html = TenazViewerController.class.getResourceAsStream("viewer.html")) {
            this.page = new String(html.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @GetMapping(produces = MediaType.TEXT_HTML_VALUE)
    public String page() {
        return page;
    }

    @GetMapping("/api/workflows")
    public Listing workflows(@RequestParam(required = false) String type,
                             @RequestParam(required = false) WorkflowStatus status,
                             @RequestParam(required = false) String q,
                             @RequestParam(defaultValue = "100") int limit) {
        Filter filter = new Filter(blankToNull(type), status, blankToNull(q));
        return new Listing(journal.countByStatus(), journal.list(filter, Math.max(1, Math.min(limit, MAX_LISTED))));
    }

    /** The id is a parameter, not part of the path, because the ids of children contain slashes. */
    @GetMapping("/api/workflow")
    public Detail workflow(@RequestParam String id) {
        List<RecordedEvent> events = journal.events(id);
        if (events.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no workflow with id " + id);
        }
        Event.WorkflowStarted started = (Event.WorkflowStarted) events.get(0).event();
        RecordedEvent last = events.get(events.size() - 1);
        WorkflowSummary summary = new WorkflowSummary(id, started.workflowType(), WorkflowStatus.after(last.event()),
                events.size(), events.get(0).recordedAt(), last.recordedAt(), started.parentId());
        return new Detail(summary, events.stream().map(this::view).toList());
    }

    private EventView view(RecordedEvent recorded) {
        Map<String, Object> data = mapper.convertValue(recorded.event(), new TypeReference<>() { });
        return new EventView(recorded.seq(), recorded.recordedAt(), recorded.event().getClass().getSimpleName(), data);
    }

    private static String blankToNull(String text) {
        return text == null || text.isBlank() ? null : text.trim();
    }
}

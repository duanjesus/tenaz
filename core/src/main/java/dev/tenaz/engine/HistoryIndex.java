package dev.tenaz.engine;

import dev.tenaz.journal.Event;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A history, indexed the way a replay consults it. Histories only grow, so the index is built
 * incrementally: a session keeps one for as long as it owns a workflow and adds events as they
 * are journaled, instead of indexing the whole history again for every replay.
 *
 * <p>An index may extend a parent. That is how a replay sees events that are about to be
 * journaled but are not durable yet, without them touching the index of what is.
 */
final class HistoryIndex {

    /** @param position where in the history the event sits, which is what orders everything */
    record Resolution(int position, Event event) {}

    private final HistoryIndex parent;
    private final int offset;
    private final Map<Integer, Resolution> commands = new HashMap<>();
    private final Map<Integer, Resolution> resolutions = new HashMap<>();
    private final Map<String, List<Resolution>> signals = new HashMap<>();
    private final Map<String, Integer> versions = new HashMap<>();
    private Resolution cancellation;
    private int size;
    private int maxCommandSeq = -1;
    private Event first;
    private Event last;

    HistoryIndex() {
        this(null);
    }

    HistoryIndex(HistoryIndex parent) {
        this.parent = parent;
        this.offset = parent == null ? 0 : parent.version();
    }

    static HistoryIndex of(List<Event> events) {
        HistoryIndex index = new HistoryIndex();
        events.forEach(index::add);
        return index;
    }

    void add(Event event) {
        Resolution at = new Resolution(offset + size++, event);
        switch (event) {
            case Event.StepScheduled e -> command(e.seq(), at);
            case Event.TimerStarted e -> command(e.seq(), at);
            case Event.SideEffectRecorded e -> command(e.seq(), at);
            case Event.ChildStarted e -> command(e.seq(), at);
            case Event.StepCompleted e -> resolutions.put(e.seq(), at);
            case Event.StepFailed e -> resolutions.put(e.seq(), at);
            case Event.TimerFired e -> resolutions.put(e.seq(), at);
            case Event.ChildCompleted e -> resolutions.put(e.seq(), at);
            case Event.ChildFailed e -> resolutions.put(e.seq(), at);
            case Event.SignalReceived e -> signals.computeIfAbsent(e.name(), k -> new ArrayList<>()).add(at);
            case Event.VersionMarked e -> versions.putIfAbsent(e.changeId(), e.version());
            case Event.CancelRequested e -> {
                // Only the first request counts; a workflow is cancelled once.
                if (cancellation() == null) {
                    cancellation = at;
                }
            }
            default -> { }
        }
        if (first == null) {
            first = event;
        }
        last = event;
    }

    private void command(int seq, Resolution at) {
        commands.put(seq, at);
        maxCommandSeq = Math.max(maxCommandSeq, seq);
    }

    int version() {
        return offset + size;
    }

    Event first() {
        return parent != null ? parent.first() : first;
    }

    Event last() {
        return last != null || parent == null ? last : parent.last();
    }

    /** The command recorded under this number and where, or null if the code has not issued it. */
    Resolution command(int seq) {
        Resolution command = commands.get(seq);
        return command != null || parent == null ? command : parent.command(seq);
    }

    Resolution resolution(int seq) {
        Resolution resolution = resolutions.get(seq);
        return resolution != null || parent == null ? resolution : parent.resolution(seq);
    }

    /** The n-th signal with this name, counting from zero, or null if it has not arrived. */
    Resolution signal(String name, int n) {
        int inherited = parent == null ? 0 : parent.signalCount(name);
        if (n < inherited) {
            return parent.signal(name, n);
        }
        List<Resolution> own = signals.getOrDefault(name, List.of());
        return n - inherited < own.size() ? own.get(n - inherited) : null;
    }

    private int signalCount(String name) {
        return (parent == null ? 0 : parent.signalCount(name)) + signals.getOrDefault(name, List.of()).size();
    }

    /** The request to cancel the workflow, or null if nobody has asked. */
    Resolution cancellation() {
        Resolution inherited = parent == null ? null : parent.cancellation();
        return inherited != null ? inherited : cancellation;
    }

    Integer markedVersion(String changeId) {
        Integer inherited = parent == null ? null : parent.markedVersion(changeId);
        return inherited != null ? inherited : versions.get(changeId);
    }

    int maxCommandSeq() {
        return parent == null ? maxCommandSeq : Math.max(maxCommandSeq, parent.maxCommandSeq());
    }
}

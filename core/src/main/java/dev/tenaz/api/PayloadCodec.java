package dev.tenaz.api;

/** Turns workflow inputs, step results and signal payloads into the text the journal stores. */
public interface PayloadCodec {

    String encode(Object value);

    <T> T decode(String payload, Class<T> type);
}

package dev.tenaz.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JacksonCodec implements PayloadCodec {

    private final ObjectMapper mapper;

    public JacksonCodec() {
        this(JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build());
    }

    public JacksonCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public String encode(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot encode " + value, e);
        }
    }

    @Override
    public <T> T decode(String payload, Class<T> type) {
        if (type == Void.class) {
            return null;
        }
        try {
            return mapper.readValue(payload, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot decode " + type.getName() + " from " + payload, e);
        }
    }
}

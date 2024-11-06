package com.neodymium.customdb;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JacksonObjectMapperSingleton {
    private static final ObjectMapper OBJECT_MAPPER = constructObjectMapper();

    private static ObjectMapper constructObjectMapper() {
        return new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}

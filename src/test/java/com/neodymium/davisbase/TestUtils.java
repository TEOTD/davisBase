package com.neodymium.davisbase;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.nio.charset.Charset;

public class TestUtils {
    private static final ObjectMapper mapper = JacksonObjectMapperSingleton.getObjectMapper();

    @SneakyThrows
    public static <T> T getObjectFromJson(String path, Class<T> genericClass) {
        Resource resource = new ClassPathResource(path);
        return mapper.readValue(resource.getInputStream(), genericClass);
    }

    @SneakyThrows
    public static <T> T getObjectFromJson(String path, TypeReference<T> typeReference) {
        Resource resource = new ClassPathResource(path);
        return mapper.readValue(resource.getInputStream(), typeReference);
    }

    @SneakyThrows
    public static String getObjectAsString(String path) {
        Resource resource = new ClassPathResource(path);
        return resource.getContentAsString(Charset.defaultCharset());
    }
}

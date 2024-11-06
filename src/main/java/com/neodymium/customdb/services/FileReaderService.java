package com.neodymium.customdb.services;

import com.neodymium.customdb.error.CustomDbException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@AllArgsConstructor
public class FileReaderService {
    public Map<Integer, String> readFile(String filePath) {
        Map<Integer, String> result = new HashMap<>();
        Path path = Path.of(filePath);

        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            String line;
            int lineNumber = 0;
            while ((line = bufferedReader.readLine()) != null) {
                result.put(lineNumber++, line);
            }
        } catch (IOException e) {
            log.error("Error while reading file at {}: {}", filePath, e.getMessage());
            throw new CustomDbException("Error while reading file: " + e.getMessage());
        }

        return Collections.unmodifiableMap(result);
    }
}

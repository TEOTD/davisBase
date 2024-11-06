package com.neodymium.customdb.config;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Slf4j
@Configuration
@AllArgsConstructor
public class DirectoryInitializer {
    private final FileProperties fileProperties;

    @PostConstruct
    public void init() {
        createDirectory(fileProperties.getInputDir(), "Input");
        createDirectory(fileProperties.getOutputDir(), "Output");
    }

    private void createDirectory(String path, String type) {
        File directory = new File(path);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                log.info("{} directory created at: {}", type, path);
            } else {
                log.error("Failed to create {} directory at: {}", type, path);
            }
        } else {
            log.info("{} directory already exists at: {}", type, path);
        }
    }
}

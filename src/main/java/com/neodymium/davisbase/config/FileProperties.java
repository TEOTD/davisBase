package com.neodymium.davisbase.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "file")
public class FileProperties {
    private String inputDir;
    private String outputDir;
    private String inputFileName;
    private String outputFileName;
    private int pageSize = 4096;
    private int noOfPages = 10;
    private int recordSize = 112;
    private int offsetSize = 2;
    private int pageHeaderSize = 2;
}

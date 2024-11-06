package com.neodymium.customdb.services;

import com.neodymium.customdb.error.CustomDbException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@AllArgsConstructor
public class CsvService {
    private final FileReaderService fileReaderService;

    private Map<String, String> getRow(String line, int rowId, String[] headers) {
        String[] values = line.split(",");
        Map<String, String> rowToHeaderMapping = new HashMap<>();
        rowToHeaderMapping.put("RowId", String.valueOf(rowId));

        for (int i = 0; i < headers.length; i++) {
            rowToHeaderMapping.put(headers[i], values[i]);
        }
        return rowToHeaderMapping;
    }

    public List<Map<String, String>> parseCsv(String filePath) {
        List<Map<String, String>> fileContent = new ArrayList<>();
        Map<Integer, String> fileContents = fileReaderService.readFile(filePath);

        if (fileContents.isEmpty()) {
            log.error("CSV file content is empty");
            throw new CustomDbException("CSV file content is empty");
        }

        String[] headers = fileContents.get(0).split(",");
        for (int rowId = 1; rowId < fileContents.size(); rowId++) {
            String line = fileContents.get(rowId);
            Map<String, String> fileRecords = getRow(line, rowId, headers);
            fileContent.add(fileRecords);
        }

        return fileContent;
    }
}

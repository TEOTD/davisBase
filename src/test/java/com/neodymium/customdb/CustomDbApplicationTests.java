package com.neodymium.customdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.neodymium.customdb.error.CustomDbException;
import com.neodymium.customdb.services.ConverterService;
import com.neodymium.customdb.services.CsvService;
import com.neodymium.customdb.services.FileReaderService;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
class CustomDbApplicationTests {

    @InjectMocks
    private final FileReaderService fileReaderService;
    @InjectMocks
    private final ConverterService converterService;
    private final CsvService csvService;

    @Test
    @SneakyThrows
    void readFileTest() {
        Resource resource = new ClassPathResource("input/employee.csv");
        Map<Integer, String> fileContent = fileReaderService.readFile(resource.getFile().getPath());
        TypeReference<Map<Integer, String>> typeReference = new TypeReference<>() {
        };
        Map<Integer, String> expectedFileContent = TestUtils.getObjectFromJson("expected/expectedFileReaderContent.json", typeReference);
        assertThat(fileContent).isEqualTo(expectedFileContent);
    }

    @Test
    @SneakyThrows
    void readFileTestIOError() {
        Resource resource = new ClassPathResource("input/employee.csv");
        String filePath = resource.getFile().getPath();
        try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class)) {
            mockedFiles.when(() -> Files.newBufferedReader(Path.of(filePath)))
                    .thenThrow(new IOException("Mocked IO Exception"));
            assertThrows(CustomDbException.class, () -> fileReaderService.readFile(filePath));
        }
    }

    @Test
    @SneakyThrows
    void parseCsvTest() {
        Resource resource = new ClassPathResource("input/employee.csv");
        String filePath = resource.getFile().getPath();
        List<Map<String, String>> fileContent = csvService.parseCsv(filePath);
        TypeReference<List<Map<String, String>>> typeReference = new TypeReference<>() {
        };
        List<Map<String, String>> expectedFileContent = TestUtils.getObjectFromJson("expected/expectedParseCsv.json", typeReference);
        assertThat(fileContent).isEqualTo(expectedFileContent);
    }

    @Test
    @SneakyThrows
    void parseEmptyCsvTest() {
        Resource resource = new ClassPathResource("input/emptyEmployee.csv");
        String filePath = resource.getFile().getPath();
        assertThrows(CustomDbException.class, () -> csvService.parseCsv(filePath));
    }

    @Test
    void successTest() {
        assertThatCode(converterService::init).doesNotThrowAnyException();
    }

    @Test
    void successTestWithString() {
        String inputFilePath = "input/employee.csv";
        String outputFilePath = "output/employee.tbl";
        assertThatCode(() -> converterService.init(inputFilePath, outputFilePath)).doesNotThrowAnyException();
    }
}
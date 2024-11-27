//package com.neodymium.customdb;
//
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.neodymium.customdb.error.DavisBaseException;
//import com.neodymium.customdb.services.FileReaderService;
//import lombok.RequiredArgsConstructor;
//import lombok.SneakyThrows;
//import lombok.extern.slf4j.Slf4j;
//import org.junit.jupiter.api.Test;
//import org.mockito.InjectMocks;
//import org.mockito.MockedStatic;
//import org.mockito.Mockito;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.core.io.ClassPathResource;
//import org.springframework.core.io.Resource;
//import org.springframework.test.context.ActiveProfiles;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertThatCode;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//
//@Slf4j
//@SpringBootTest
//@ActiveProfiles("test")
//@RequiredArgsConstructor(onConstructor_ = {@Autowired})
//class DavisBaseApplicationTests {
//
//    @InjectMocks
//    private final FileReaderService fileReaderService;
//    @InjectMocks
//    private final ConverterService converterService;
//    private final CsvService csvService;
//
//    @Test
//    @SneakyThrows
//    void readFileTest() {
//        Resource resource = new ClassPathResource("input/employee.csv");
//        Map<Integer, String> fileContent = fileReaderService.readFile(resource.getFile().getPath());
//        TypeReference<Map<Integer, String>> typeReference = new TypeReference<>() {
//        };
//        Map<Integer, String> expectedFileContent = TestUtils.getObjectFromJson("expected/expectedFileReaderContent.json", typeReference);
//        assertThat(fileContent).isEqualTo(expectedFileContent);
//    }
//
//    @Test
//    @SneakyThrows
//    void readFileTestIOError() {
//        Resource resource = new ClassPathResource("input/employee.csv");
//        String filePath = resource.getFile().getPath();
//        try (MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class)) {
//            mockedFiles.when(() -> Files.newBufferedReader(Path.of(filePath)))
//                    .thenThrow(new IOException("Mocked IO Exception"));
//            assertThrows(DavisBaseException.class, () -> fileReaderService.readFile(filePath));
//        }
//    }
//
//    @Test
//    @SneakyThrows
//    void parseCsvTest() {
//        Resource resource = new ClassPathResource("input/employee.csv");
//        String filePath = resource.getFile().getPath();
//        List<Map<String, String>> fileContent = csvService.parseCsv(filePath);
//        TypeReference<List<Map<String, String>>> typeReference = new TypeReference<>() {
//        };
//        List<Map<String, String>> expectedFileContent = TestUtils.getObjectFromJson("expected/expectedParseCsv.json", typeReference);
//        assertThat(fileContent).isEqualTo(expectedFileContent);
//    }
//
//    @Test
//    @SneakyThrows
//    void parseEmptyCsvTest() {
//        Resource resource = new ClassPathResource("input/emptyEmployee.csv");
//        String filePath = resource.getFile().getPath();
//        assertThrows(DavisBaseException.class, () -> csvService.parseCsv(filePath));
//    }
//
//    @Test
//    void successTest() {
//        assertThatCode(converterService::init).doesNotThrowAnyException();
//    }
//
//    @Test
//    void successTestWithString() {
//        String inputFilePath = "input/employee.csv";
//        String outputFilePath = "output/employee.tbl";
//        assertThatCode(() -> converterService.init(inputFilePath, outputFilePath)).doesNotThrowAnyException();
//    }
//
//    @Test
//    @SneakyThrows
//    void outPutComparisonTest() {
//        Resource actualOutput = new ClassPathResource("output/employee.tbl");
//        Resource expectedOutput = new ClassPathResource("output/sorted.tbl");
//        BufferedReader actualReader = new BufferedReader(new InputStreamReader(
//                Objects.requireNonNull(actualOutput.getInputStream()), StandardCharsets.UTF_8));
//        BufferedReader expectedReader = new BufferedReader(new InputStreamReader(
//                Objects.requireNonNull(expectedOutput.getInputStream()), StandardCharsets.UTF_8));
//        String actualLine;
//        String expectedLine;
//        int lineNumber = 1;
//        while ((actualLine = actualReader.readLine()) != null && (expectedLine = expectedReader.readLine()) != null) {
//            byte[] actualBytes = actualLine.getBytes(StandardCharsets.UTF_8);
//            byte[] expectedBytes = expectedLine.getBytes(StandardCharsets.UTF_8);
//            assertThat(actualBytes.length)
//                    .withFailMessage("Line %d: Length mismatch - expected %d but got %d",
//                            lineNumber, expectedBytes.length, actualBytes.length)
//                    .isEqualTo(expectedBytes.length);
//            for (int i = 0; i < actualBytes.length; i++) {
//                assertThat(actualBytes[i])
//                        .withFailMessage("Line %d, Byte %d: Expected %d but got %d",
//                                lineNumber, i, expectedBytes[i], actualBytes[i])
//                        .isEqualTo(expectedBytes[i]);
//            }
//            lineNumber++;
//        }
//        assertThat(actualReader.readLine())
//                .withFailMessage("Actual output has more lines than expected")
//                .isNull();
//        assertThat(expectedReader.readLine())
//                .withFailMessage("Expected output has more lines than actual")
//                .isNull();
//    }
//}
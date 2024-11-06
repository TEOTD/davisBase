package com.neodymium.customdb.services;

import com.neodymium.customdb.config.FileProperties;
import com.neodymium.customdb.models.Employee;
import com.neodymium.customdb.models.Page;
import com.neodymium.customdb.models.Table;
import com.neodymium.customdb.models.TableRecord;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Service
@AllArgsConstructor
public class ConverterService {
    private final CsvService csvService;
    private final FileProperties fileProperties;

    public void init() {
        Path inputFilePath = Path.of(fileProperties.getInputDir(), fileProperties.getInputFileName()).toAbsolutePath();
        Path outputFilePath = Path.of(fileProperties.getOutputDir(), fileProperties.getOutputFileName()).toAbsolutePath();
        init(inputFilePath, outputFilePath);
    }

    public void init(String inputFilePath, String outputFilePath) {
        init(Path.of(inputFilePath).toAbsolutePath(), Path.of(outputFilePath).toAbsolutePath());
    }

    public void init(Path inputFilePath, Path outputFilePath) {
        List<Map<String, String>> fileContentMapping = csvService.parseCsv(inputFilePath.toString());
        Map<Integer, Page<Employee>> pages = fileContentMapping.parallelStream()
                .map(this::createEmployee)
                .sorted(Comparator.comparingInt(Employee::rowId))
                .collect(Collectors.groupingBy(
                        employee -> Integer.parseInt(employee.getPrimaryKey()) % 10,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                this::createPage
                        )
                ));

        Table<Employee> employeeTable = new Table<>(
                fileProperties.getNoOfPages(),
                fileProperties.getPageSize(),
                fileProperties.getRecordSize(),
                outputFilePath.toString(),
                pages);

        employeeTable.init();
        employeeTable.saveToFile();
    }

    private <T extends TableRecord> Page<T> createPage(List<T> tableRecords) {
        return new Page<>(tableRecords);
    }

    private Employee createEmployee(Map<String, String> recordMap) {
        int rowId = Integer.parseInt(recordMap.get("RowId"));
        String ssn = recordMap.get("Ssn");
        String fname = recordMap.get("Fname");
        char minit = Optional.ofNullable(recordMap.get("Minit"))
                .filter(s -> !s.isEmpty())
                .map(s -> s.charAt(0))
                .orElse('\0');
        String lname = recordMap.get("Lname");
        String bdate = recordMap.get("Bdate");
        String address = recordMap.get("Address");
        char sex = Optional.ofNullable(recordMap.get("Sex")).map(s -> s.charAt(0)).orElse('\0');
        int salary = Integer.parseInt(recordMap.getOrDefault("Salary", "0"));
        short dno = Short.parseShort(recordMap.getOrDefault("Dno", "0"));
        return new Employee(rowId, ssn, fname, minit, lname, bdate, address, sex, salary, dno, (byte) 0);
    }
}

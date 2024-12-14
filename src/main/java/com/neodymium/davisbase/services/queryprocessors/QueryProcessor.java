package com.neodymium.davisbase.services.queryprocessors;

import org.springframework.stereotype.Service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@AllArgsConstructor
public class QueryProcessor {

    private final DDLProcessor ddlProcessor;
    private final DMLProcessor dmlProcessor;
    private final DQLProcessor dqlProcessor;

    public void process(String command) {
        if (command == null || command.isBlank()) {
            log.warn("Command is empty or null.");
            return;
        }

        String trimmedCommand = command.trim();
        String commandType = getCommandType(trimmedCommand);

        try {
            switch (commandType) {
                case "DDL" -> ddlProcessor.process(trimmedCommand);
                case "DML" -> dmlProcessor.process(trimmedCommand);
                case "DQL" -> dqlProcessor.process(trimmedCommand);
                default -> log.warn("Unknown command type: {}", trimmedCommand);
            }
        } catch (Exception e) {
            log.error("Error processing command: {}", trimmedCommand, e);
        }
    }

    public boolean isTerminationCommand(String command) {
        return switch (command == null ? "" : command.trim().toUpperCase()) {
            case "EXIT;", "QUIT;" -> true;
            default -> false;
        };
    }

    private String getCommandType(String command) {
        command = command.toUpperCase();
        if (command.startsWith("CREATE") || command.startsWith("DROP") || command.startsWith("ALTER")) {
            return "DDL";
        } else if (command.startsWith("INSERT") || command.startsWith("UPDATE") || command.startsWith("DELETE")|| command.startsWith("DROP")) {
            return "DML";
        } else if (command.startsWith("SELECT") || command.startsWith("SHOW") || command.startsWith("HELP")) {
            return "DQL";
        } else {
            return "UNKNOWN";
        }
    }
}
package com.neodymium.davisbase.services.queryprocessors;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@AllArgsConstructor
public class QueryProcessor {

    public final DDLProcessor ddlProcessor;
    public final DMLProcessor dmlProcessor;
    public final DQLProcessor dqlProcessor;

    public QueryProcessor() {
        this.ddlProcessor = new DDLProcessor();
        this.dmlProcessor = new DMLProcessor();
        this.dqlProcessor = new DQLProcessor();
    }

    public void process(String command) {
        // Trim and sanitize the command
        command = command.trim();

        // Check for termination command
        if (isTerminationCommand(command)) {
            System.out.println("Exiting the system.");
            System.exit(0);
        }

        // Determine the type of command and delegate to the appropriate processor
        String commandType = getCommandType(command);

        try {
            switch (commandType) {
                case "DDL":
                    ddlProcessor.processDDL(command);
                    break;
                case "DML":
                    dmlProcessor.processDML(command);
                    break;
                case "DQL":
                    dqlProcessor.processDQL(command);
                    break;
                default:
                    System.out.println("Unknown command type: " + command);
                    break;
            }
        } catch (Exception e) {
            System.out.println("Error processing command: " + e.getMessage());
        }
    }

    public boolean isTerminationCommand(String command) {
        // Check for standard termination commands like EXIT or QUIT
        return command.equalsIgnoreCase("EXIT;") || command.equalsIgnoreCase("QUIT;");
    }

    private String getCommandType(String command) {
        // Analyze the command and return its type
        command = command.toUpperCase();

        if (command.startsWith("CREATE") || command.startsWith("DROP") || command.startsWith("ALTER")) {
            return "DDL";
        } else if (command.startsWith("INSERT") || command.startsWith("UPDATE") || command.startsWith("DELETE")) {
            return "DML";
        } else if (command.startsWith("SELECT")) {
            return "DQL";
        } else if (command.startsWith("BEGIN") || command.startsWith("COMMIT") || command.startsWith("ROLLBACK")) {
            return "TCL";
        } else if (command.startsWith("GRANT") || command.startsWith("REVOKE")) {
            return "VDL";
        } else {
            return "UNKNOWN";
        }
    }
}
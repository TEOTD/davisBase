<<<<<<< HEAD
package com.neodymium.davisbase.controller;

import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.services.queryprocessors.QueryProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

import static com.neodymium.davisbase.constants.Constants.DATABASE_COMMAND_TERMINATION_SYMBOL;
import static com.neodymium.davisbase.constants.Constants.DATABASE_EXITING_STRING;
import static com.neodymium.davisbase.constants.Constants.DATABASE_LINE_SEPARATOR;
import static com.neodymium.davisbase.constants.Constants.DATABASE_PROMPT;
import static com.neodymium.davisbase.constants.Constants.DATABASE_SUPPORTED_COMMANDS;
import static com.neodymium.davisbase.constants.Constants.DATABASE_VERSION_STRING;
import static com.neodymium.davisbase.constants.Constants.DATABASE_WELCOME_MESSAGE;

@Slf4j
@Component
@AllArgsConstructor
public class CMDController implements CommandLineRunner {
    private final QueryProcessor queryProcessor;

    private static void displaySplashScreen() {
        log.info("\n{}\n{}\n{}\n{}\n{}\n",
                DATABASE_LINE_SEPARATOR,
                DATABASE_WELCOME_MESSAGE,
                DATABASE_VERSION_STRING,
                DATABASE_SUPPORTED_COMMANDS,
                DATABASE_LINE_SEPARATOR);
    }

    @Override
    public void run(String... args) {
        displaySplashScreen();
        try (Scanner scanner = new Scanner(System.in).useDelimiter(DATABASE_COMMAND_TERMINATION_SYMBOL)) {
            while (true) {
                System.out.print(DATABASE_PROMPT);
                String input = scanner.next();
                String parsedInput = parseInput(input);
                queryProcessor.process(parsedInput);
                if (queryProcessor.isTerminationCommand(parsedInput)) {
                    log.info(DATABASE_EXITING_STRING);
                    break;
                }
            }
        } catch (Exception e) {
            log.error("An error occurred while processing commands: ", e);
            throw new DavisBaseException("An error occurred while processing commands: " + e);
        }
    }

    private String parseInput(String input) {
        return input.replaceAll("\\s+", " ")
                .replace("\r", "")
                .trim();
    }
}
=======
package com.neodymium.davisbase.controller;

import com.neodymium.davisbase.error.DavisBaseException;
import com.neodymium.davisbase.services.queryprocessors.QueryProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Scanner;

import static com.neodymium.davisbase.constants.Constants.DATABASE_COMMAND_TERMINATION_SYMBOL;
import static com.neodymium.davisbase.constants.Constants.DATABASE_EXITING_STRING;
import static com.neodymium.davisbase.constants.Constants.DATABASE_LINE_SEPARATOR;
import static com.neodymium.davisbase.constants.Constants.DATABASE_PROMPT;
import static com.neodymium.davisbase.constants.Constants.DATABASE_SUPPORTED_COMMANDS;
import static com.neodymium.davisbase.constants.Constants.DATABASE_VERSION_STRING;
import static com.neodymium.davisbase.constants.Constants.DATABASE_WELCOME_MESSAGE;

@Slf4j
@Component
@AllArgsConstructor
public class CMDController implements CommandLineRunner {
    private final QueryProcessor queryProcessor;

    private static void displaySplashScreen() {
        log.info("\n{}\n{}\n{}\n{}\n{}\n",
                DATABASE_LINE_SEPARATOR,
                DATABASE_WELCOME_MESSAGE,
                DATABASE_VERSION_STRING,
                DATABASE_SUPPORTED_COMMANDS,
                DATABASE_LINE_SEPARATOR);
    }

    @Override
    public void run(String... args) {
        displaySplashScreen();
        try (Scanner scanner = new Scanner(System.in).useDelimiter(DATABASE_COMMAND_TERMINATION_SYMBOL)) {
            while (true) {
                System.out.print(DATABASE_PROMPT);
                String input = scanner.next();
                String parsedInput = parseInput(input);
                queryProcessor.process(parsedInput);
                if (queryProcessor.isTerminationCommand(parsedInput)) {
                    log.info(DATABASE_EXITING_STRING);
                    break;
                }
            }
        } catch (Exception exception) {
            log.error("An error occurred while processing commands: ", exception);
            throw new DavisBaseException(exception);
        }
    }

    private String parseInput(String input) {
        return input.replaceAll("\\s+", " ")
                .replace("\r", "")
                .trim();
    }
}
>>>>>>> 5689cd5b61352fa8d05eec945a1947c15e453842

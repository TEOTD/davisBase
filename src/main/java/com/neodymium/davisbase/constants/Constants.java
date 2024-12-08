package com.neodymium.davisbase.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Constants {
    //Add all hardcoded constant properties here.

    public static final String BLANK_SPACE = " ";
    public static final String DATABASE_NAME = "davisBase";
    public static final String DATABASE_NAME_STRING = "Davis Base";
    public static final String DATABASE_PROMPT = DATABASE_NAME + "> ";
    public static final String DATABASE_COMMAND_TERMINATION_SYMBOL = ";";
    public static final String DATABASE_EXITING_STRING = "Exiting...";
    public static final String DATABASE_LINE_SEPARATOR = "*".repeat(Math.max(0, 80));

    public static final String DATABASE_VERSION = "1.0";
    public static final String DATABASE_VERSION_STRING = "DavisBase Version: " + DATABASE_VERSION;
    public static final String DATABASE_WELCOME_MESSAGE = "Welcome to " + DATABASE_NAME_STRING + "!!";
    public static final String DATABASE_SUPPORTED_COMMANDS = "Type \"help;\" to display supported commands.";
    public static final String DATA_DIR = "./";
}

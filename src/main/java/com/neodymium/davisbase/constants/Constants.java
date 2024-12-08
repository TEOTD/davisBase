package com.neodymium.davisbase.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.File;

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

    //directories
    public static final String CATALOG_DIRECTORY = "catalogs";
    public static final String TABLE_FILE_EXTENSION = ".tbl";
    public static final String INDEX_FILE_EXTENSION = ".ndx";
    public static final String TABLE_DIRECTORY = "tables";
    public static final String INDEX_DIRECTORY = "indexes";
    public static final String TABLE_CATALOG = CATALOG_DIRECTORY + File.separator + "davisbase_tables" + TABLE_FILE_EXTENSION;
    public static final String COLUMN_CATALOG = CATALOG_DIRECTORY + File.separator + "davisbase_columns" + TABLE_FILE_EXTENSION;
    public static final Integer PAGE_SIZE = 512;
    public static final String DATA_DIR = "./";
}

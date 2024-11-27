package com.neodymium.davisbase.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseInitializer {
    //todo: Initialize directory where all the files will be stored if not exists
    //todo: else in data directory check if there is any existing table and columns in catalog
    //todo: if any one file is missing or both files are missing initialize the files and the directory
    //todo: if any problem occurs throw exception and terminate the program
}

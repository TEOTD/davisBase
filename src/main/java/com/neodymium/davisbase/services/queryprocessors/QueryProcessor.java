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

    public void process(String command) {
        //todo: make generic function to process the command
        //todo: breakdown the command and then use switch case to call different processors.
    }

    public boolean isTerminationCommand(String command) {
        //todo: make termination function
        return false;
    }
}

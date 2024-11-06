package com.neodymium.customdb;

import com.neodymium.customdb.services.ConverterService;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Profile("!test")
public class StartupRunner implements ApplicationRunner, CommandLineRunner {

    private final ConverterService converterService;

    @Override
    public void run(ApplicationArguments args) {
        String[] sourceArgs = args.getSourceArgs();
        if (sourceArgs.length == 0) {
            converterService.init();
        } else if (sourceArgs.length >= 2) {
            converterService.init(sourceArgs[0], sourceArgs[1]);
        }
    }

    @Override
    public void run(String... args) {
        if (args.length == 0) {
            converterService.init();
        } else if (args.length >= 2) {
            converterService.init(args[0], args[1]);
        }
    }
}

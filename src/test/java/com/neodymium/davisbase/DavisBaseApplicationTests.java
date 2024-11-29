package com.neodymium.davisbase;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
class DavisBaseApplicationTests {
    //todo: need to write test cases
}
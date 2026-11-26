package com.aerosync.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class TimeConfig {

    /** Single time source for the app; tests swap in a fixed or mutable clock. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}

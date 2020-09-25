package com.dbist.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource({ "classpath:/WEB-INF/application-context.xml", "classpath:/WEB-INF/dataSource-context.xml" })
public class DemoWebappApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoWebappApplication.class, args);
    }
}

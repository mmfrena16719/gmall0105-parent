package com.yang.gmall0105.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.yang.gmall0105.publisher.mapper")
public class GmallPublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(GmallPublisherApplication.class, args);
	}

}

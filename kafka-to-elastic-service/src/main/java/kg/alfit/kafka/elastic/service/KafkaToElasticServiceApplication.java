package kg.alfit.kafka.elastic.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "kg.alfit")
public class KafkaToElasticServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaToElasticServiceApplication.class, args);
    }
}

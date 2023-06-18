package kg.alfit.twitter.to.kafka.service;

import kg.alfit.config.TwitterToKafkaServiceConfig;
import kg.alfit.twitter.to.kafka.service.init.StreamInitializer;
import kg.alfit.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication(scanBasePackages = "kg.alfit")
@Slf4j
@RequiredArgsConstructor
public class TwitterToKafkaApplicationService implements CommandLineRunner {
    private final StreamRunner streamRunner;
    private final StreamInitializer streamInitializer;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplicationService.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
       log.info("App starts");
       streamInitializer.init();
       streamRunner.start();
    }
}

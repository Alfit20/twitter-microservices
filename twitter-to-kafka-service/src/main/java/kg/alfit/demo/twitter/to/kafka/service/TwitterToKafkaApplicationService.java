package kg.alfit.demo.twitter.to.kafka.service;

import kg.alfit.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfig;
import kg.alfit.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class TwitterToKafkaApplicationService implements CommandLineRunner {
    private final TwitterToKafkaServiceConfig twitterToKafkaServiceConfig;
    private final StreamRunner streamRunner;

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplicationService.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
       log.info("App starts");
       log.info(Arrays.toString(twitterToKafkaServiceConfig.getTwitterKeywords().toArray(new String[] {})));
       streamRunner.start();
    }
}

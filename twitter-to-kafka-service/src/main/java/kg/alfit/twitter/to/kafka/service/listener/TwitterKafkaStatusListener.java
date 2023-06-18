package kg.alfit.twitter.to.kafka.service.listener;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import kg.alfit.config.KafkaConfigData;
import kg.alfit.kafka.producer.config.service.KafkaProducer;
import kg.alfit.twitter.to.kafka.service.transformer.TwitterStatusAvroTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
@Slf4j
@RequiredArgsConstructor
public class TwitterKafkaStatusListener extends StatusAdapter {
    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;
    private final TwitterStatusAvroTransformer twitterStatusAvroTransformer;

    @Override
    public void onStatus(Status status) {
        log.info("Twitter status with text {}", status.getText());
        TwitterAvroModel twitterAvroModel = twitterStatusAvroTransformer.getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(),twitterAvroModel);
    }
}

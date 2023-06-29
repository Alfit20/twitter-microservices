package kg.alfit.kafka.elastic.service.transformer;

import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import kg.alfit.elastic.model.index.impl.TwitterIndexModel;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class AvroToElasticModelTransformer {

    public List<TwitterIndexModel> getElasticModels(List<TwitterAvroModel> avroModels) {
        return avroModels.stream()
                .map(avroModel -> TwitterIndexModel.builder()
                        .userId(avroModel.getUserId())
                        .id(String.valueOf(avroModel.getId()))
                        .text(avroModel.getText())
                        .createdAt(LocalDateTime.ofInstant(Instant.ofEpochMilli
                                (avroModel.getCreatedAt()), ZoneId.systemDefault()))
                        .build())
                .collect(Collectors.toList());
    }
}

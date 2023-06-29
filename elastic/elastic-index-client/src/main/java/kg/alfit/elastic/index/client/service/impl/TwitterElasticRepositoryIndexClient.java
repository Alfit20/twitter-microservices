package kg.alfit.elastic.index.client.service.impl;

import kg.alfit.elastic.index.client.repository.TwitterElasticsearchIndexRepository;
import kg.alfit.elastic.index.client.service.ElasticIndexClient2;
import kg.alfit.elastic.model.index.impl.TwitterIndexModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "elastic.config.is-repository", havingValue = "true", matchIfMissing = true)
@Slf4j
@RequiredArgsConstructor
public class TwitterElasticRepositoryIndexClient implements ElasticIndexClient2<TwitterIndexModel> {
    private final TwitterElasticsearchIndexRepository twitterElasticsearchIndexRepository;

    @Override
    public List<String> save(List<TwitterIndexModel> documents) {
        List<TwitterIndexModel> repositoryResponse =
                (List<TwitterIndexModel>) twitterElasticsearchIndexRepository.saveAll(documents);
        List<String> ids = repositoryResponse.stream().map(TwitterIndexModel::getId).collect(Collectors.toList());
        log.info("Documents indexed successfully with type: {} and ids: {}",
                TwitterIndexModel.class.getName(), ids);
        return ids;

    }
}

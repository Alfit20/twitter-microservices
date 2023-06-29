package kg.alfit.elastic.index.client.service.impl;

import kg.alfit.config.ElasticConfigData;
import kg.alfit.elastic.index.client.service.ElasticIndexClient;
import kg.alfit.elastic.index.client.util.ElasticIndexUtil;
import kg.alfit.elastic.model.index.impl.TwitterIndexModel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexedObjectInformation;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
@ConditionalOnProperty(name = "elastic.config.is-repository", havingValue = "false")
public class TwitterElasticIndexClient implements ElasticIndexClient<TwitterIndexModel> {
    private final ElasticConfigData elasticConfigData;
    private final ElasticsearchOperations elasticsearchOperations;
    private final ElasticIndexUtil<TwitterIndexModel> elasticIndexUtil;

    @Override
    public List<IndexedObjectInformation> save(List<TwitterIndexModel> documents) {
        List<IndexQuery> indexQueries = elasticIndexUtil.getIndexQueries(documents);
        List<IndexedObjectInformation> documentIds = elasticsearchOperations.bulkIndex(
                indexQueries,
                IndexCoordinates.of(elasticConfigData.getIndexName())
        );
        log.info("Documents indexed successfully with type: {} and ids: {}",
                TwitterIndexModel.class.getName(), documents);
        return documentIds;
    }
}

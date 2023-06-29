package kg.alfit.elastic.index.client.service;

import kg.alfit.elastic.model.index.IndexModel;
import org.springframework.data.elasticsearch.core.IndexedObjectInformation;

import java.util.List;

public interface ElasticIndexClient<T extends IndexModel> {
    List<IndexedObjectInformation> save(List<T> documents);
}

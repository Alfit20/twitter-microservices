package kg.alfit.elastic.index.client.service;

import kg.alfit.elastic.model.index.IndexModel;

import java.util.List;

public interface ElasticIndexClient2 <T extends IndexModel> {
    List<String> save(List<T> documents);
}

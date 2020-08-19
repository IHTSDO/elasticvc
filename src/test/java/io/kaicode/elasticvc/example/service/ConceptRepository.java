package io.kaicode.elasticvc.example.service;

import io.kaicode.elasticvc.example.domain.Concept;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface ConceptRepository extends ElasticsearchRepository<Concept, String> {

}

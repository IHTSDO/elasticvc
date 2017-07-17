package io.kaicode.elasticvc.example.service;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.example.domain.Concept;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;

public interface ConceptRepository extends ElasticsearchCrudRepository<Concept, String> {

}

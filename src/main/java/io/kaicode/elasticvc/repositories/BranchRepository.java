package io.kaicode.elasticvc.repositories;

import io.kaicode.elasticvc.domain.Branch;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface BranchRepository extends ElasticsearchRepository<Branch, String> {

}

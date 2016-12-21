package io.kaicode.elasticvc.repositories;

import io.kaicode.elasticvc.domain.Branch;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;

public interface BranchRepository extends ElasticsearchCrudRepository<Branch, String> {

}

package io.kaicode.elasticvc.example.service;

import io.kaicode.elasticvc.api.BranchCriteria;
import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.api.ComponentService;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.example.domain.Concept;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Service
public class ConceptService extends ComponentService {

	// Used to open commits
	@Autowired
	private BranchService branchService;

	// Used to create views of a branch
	@Autowired
	private VersionControlHelper versionControlHelper;

	// Used to search
	@Autowired
	private ElasticsearchOperations elasticsearchTemplate;

	// Example domain entity repository
	@Autowired
	private ConceptRepository conceptRepository;

	public void createConcept(Concept concept, String branch) {
		try (Commit commit = branchService.openCommit(branch)) {
			// Mark new entity as changed so that it's persisted
			concept.markChanged();
			// Delegate actual document save to API ComponentService
			doSaveBatchComponents(Collections.singleton(concept), commit, Concept.FIELD_ID, conceptRepository);

			// Once everything within a commit has been persisted successfully we mark the commit as successful
			// When we leave the try block the commit will autoclose making the commit content available to view
			commit.markSuccessful();
		}
	}

	public Concept findConcept(String id, String branchPath) {
		// The VersionControlHelper is used to give us a view of content on a branch
		// This view includes content on parent branches up to the point of last rebase
		// it excludes content on parent branches which has been deleted on this branch
		BranchCriteria branchCriteria = versionControlHelper.getBranchCriteria(branchPath);

		// Create a query
		NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
						// Always include the branch criteria
						.must(branchCriteria.getEntityBranchCriteria(Concept.class))
						// Also add any other required clauses
						.must(termQuery(Concept.FIELD_ID, id))
				).build();
		List<Concept> concepts = elasticsearchTemplate.queryForList(nativeSearchQuery, Concept.class);
		return !concepts.isEmpty() ? concepts.get(0) : null;
	}
}

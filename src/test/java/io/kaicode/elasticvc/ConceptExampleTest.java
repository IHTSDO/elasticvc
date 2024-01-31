package io.kaicode.elasticvc;

import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.api.ComponentService;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.example.domain.Concept;
import io.kaicode.elasticvc.example.service.ConceptService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.index.Settings;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;

import java.util.*;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;
import static io.kaicode.elasticvc.helper.QueryHelper.termQuery;
import static org.junit.jupiter.api.Assertions.*;


class ConceptExampleTest extends AbstractTest {

	@Autowired
	private ConceptService conceptService;

	@Autowired
	private BranchService branchService;

	@Test
	void testBranchDoesNotExist() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> conceptService.findConcept("1", "MAIN"));
	}

	@Test
	void testCreateFindUpdateOnMainBranch() {
		String branch = "MAIN";
		branchService.create(branch);

		assertNull(conceptService.findConcept("1", branch), "Domain entity is not created yet.");

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branch);

		Concept conceptFromRepository = conceptService.findConcept("1", branch);
		assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals(Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"), "There should be no versions replaced on MAIN");

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branch);

		assertEquals(Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"), "There should be no versions replaced on MAIN");
	}

	@Test
	void testCreateFindUpdateOnChildOfMainBranch() {
		String branch = "MAIN/A";
		branchService.create("MAIN");
		branchService.create(branch);

		assertNull(conceptService.findConcept("1", branch), "Domain entity is not created yet.");

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branch);

		Concept conceptFromRepository = conceptService.findConcept("1", branch);
		assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals(Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"), "There should be no versions replaced on MAIN/A");

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branch);

		assertEquals(Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"),
				"There should be no versions replaced on MAIN/A because the concept does not exist on the parent branch.");


		// Create concept 2 on MAIN
		Concept concept2 = new Concept("2", "Concept 2");
		conceptService.createUpdateConcept(concept2, "MAIN");

		// Rebase MAIN/A to make concept 2 visible
		try (Commit rebaseCommit = branchService.openRebaseCommit(branch)) {
			rebaseCommit.markSuccessful();
		}
		assertEquals("Concept 2", conceptService.findConcept("2", branch).getTerm());

		// Update concept 2 on MAIN/A
		concept2.setTerm("Something new");
		conceptService.createUpdateConcept(concept2, branch);

		assertEquals(1, branchService.findLatest(branch).getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size(),
				"Branch MAIN/A should now record 1 version replaced because concept 2 exists on the parent branch." +
						"This allows the version control system to hide the version on MAIN when finding domain entities.");

		try (Commit commit = branchService.openPromotionCommit("MAIN", "MAIN/A")) {
			commit.markSuccessful();
		}

		assertEquals(0, branchService.findLatest("MAIN/A").getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size(),
				"Branch MAIN/A should now record 0 versions replaced because it's content changes have been promoted.");
		assertEquals(0, branchService.findLatest("MAIN").getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size(),
				"Branch MAIN always have 0 versions replaced because it has no parents so this is not needed during content selection.");

	}

	@Test
	void testCommitRollback() {
		String branchPath = "MAIN/A";
		branchService.create("MAIN");
		branchService.create(branchPath);

		assertNull(conceptService.findConcept("1", branchPath), "Domain entity is not created yet.");

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branchPath);

		Concept conceptFromRepository = conceptService.findConcept("1", branchPath);
		assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals(Collections.emptySet(), branchService.findLatest(branchPath).getVersionsReplaced().get("Concept"), "There should be no versions replaced on MAIN/A");

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branchPath);

		assertEquals(Collections.emptySet(), branchService.findLatest(branchPath).getVersionsReplaced().get("Concept"),
				"There should be no versions replaced on MAIN/A because the concept does not exist on the parent branch.");


		// Create concept 2 on MAIN
		Concept concept2 = new Concept("2", "Concept 2");
		conceptService.createUpdateConcept(concept2, "MAIN");

		// Rebase MAIN/A to make concept 2 visible
		try (Commit rebaseCommit = branchService.openRebaseCommit(branchPath)) {
			rebaseCommit.markSuccessful();
		}
		assertEquals("Concept 2", conceptService.findConcept("2", branchPath).getTerm());

		Branch branchVersionBeforeUpdate = branchService.findLatest(branchPath);

		// Update concept 2 on MAIN/A
		concept2.setTerm("Something new");
		conceptService.createUpdateConcept(concept2, branchPath);

		assertEquals(1, branchService.findLatest(branchPath).getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size(),
				"Branch MAIN/A should now record 1 version replaced because concept 2 exists on the parent branch." +
						"This allows the version control system to hide the version on MAIN when finding domain entities.");

		// Rollback commit on MAIN/A
		Branch branchVersion = branchService.findLatest(branchPath);
		branchService.rollbackCompletedCommit(branchVersion, Collections.singletonList(Concept.class));

		branchVersion = branchService.findLatest(branchPath);
		assertEquals(branchVersionBeforeUpdate.getHead(), branchVersion.getHead(), "The head timepoint should now match the previous commit.");
		assertEquals(Collections.emptySet(), branchVersion.getVersionsReplaced().get("Concept"), "There should now be no versions replaced on MAIN/A because the commit was rolled back.");
	}


	@Test
	void testFetchFieldsOnlyWithSourceFilter() {
		// Create concept
		branchService.create("MAIN");
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, "MAIN");
		NativeQueryBuilder queryBuilder = new NativeQueryBuilder();
		// After upgrading to 5.1.16 withFields is no longer working as expected
		// Use withSourceFilter instead
		queryBuilder.withSourceFilter(new FetchSourceFilter(new String[] { Concept.FIELD_ID }, null));
		queryBuilder.withQuery(bool(b -> b.must(termQuery(Concept.FIELD_ID, "1"))));

		SearchHits<Concept> searchHits = elasticsearchOperations.search(queryBuilder.build(), Concept.class);
		assertEquals(1, searchHits.getTotalHits());
		Concept concept = searchHits.getSearchHit(0).getContent();
		assertNotNull(concept);
		assertEquals("1", concept.getId());
		assertNull(concept.getStart());
		assertNull(concept.getTerm());
	}

	@Test
	void testIndexConfigsWithDefaultSettings() {
		IndexCoordinates indexCoordinates = elasticsearchOperations.getIndexCoordinatesFor(Concept.class);
		assertEquals("test_concept", indexCoordinates.getIndexName());
		Settings settings = elasticsearchOperations.indexOps(indexCoordinates).getSettings();
		// Default settings
		assertEquals("1", settings.getString("index.number_of_shards"));
		assertEquals("1", settings.getString("index.number_of_replicas"));
	}

	@Test
	void testCreateIndexWithExternalConfigs() {
		try {
			Map<String, Object> settings = new HashMap<>();
			settings.put("index.number_of_shards", "3");
			settings.put("index.number_of_replicas", "1");
			ComponentService.initialiseIndexAndMappingForPersistentClasses(
					true,
					elasticsearchOperations,
					settings,
					Concept.class
			);
			IndexCoordinates indexCoordinates = elasticsearchOperations.getIndexCoordinatesFor(Concept.class);
			assertEquals("test_concept", indexCoordinates.getIndexName());
			Settings shardSettings = elasticsearchOperations.indexOps(indexCoordinates).getSettings();
			assertEquals("3", shardSettings.getString("index.number_of_shards"));
			assertEquals("1", shardSettings.getString("index.number_of_replicas"));
		} finally {
			// Reset to default
			ComponentService.initialiseIndexAndMappingForPersistentClasses(
					true,
					elasticsearchOperations,
					Concept.class
			);
		}
	}


	@Test
	void testSavingConceptsWithSeparateIndex() {
		// Create a concept in MAIN
		branchService.create("MAIN");
		conceptService.createUpdateConcept(new Concept("1", "Concept in MAIN"), "MAIN");
		Concept concept = conceptService.findConcept("1", "MAIN");

		// Create an extension branch with separate index for concepts in branch metadata
		Map<String, Object> metadata = new HashMap<>();
		metadata.put(VersionControlHelper.VC_SEPARATE_INDEX_ENTITY_CLASS_NAMES, List.of("Concept"));
		branchService.create("MAIN/EXTENSION-A", metadata);

		// Update the concept in the extension branch and save
		concept.setTerm("Updated by extension branch");
		conceptService.createUpdateConcept(concept, "MAIN/EXTENSION-A");

		// Check that the concept is saved in the extension branch index and no versions replaced are recorded
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A");
		assertEquals("Updated by extension branch", concept.getTerm());
		Map<String, Set<String>> versionsReplaced =  branchService.findLatest("MAIN/EXTENSION-A").getVersionsReplaced();
		assertEquals(Collections.emptyMap(), versionsReplaced);
	}

	@AfterEach
	void tearDown() {
		branchService.deleteAll();
		conceptService.deleteAll();
	}
}

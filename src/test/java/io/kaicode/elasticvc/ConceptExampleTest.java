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
		queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{Concept.FIELD_ID}, null));
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
	void testSavingConcepts() {
		// Create a concept in MAIN
		branchService.create("MAIN");
		conceptService.createUpdateConcept(new Concept("1", "Concept in MAIN"), "MAIN");
		Concept concept = conceptService.findConcept("1", "MAIN");

		branchService.create("MAIN/EXTENSION-A");
		// Update the concept in the extension branch and save
		concept.setTerm("Updated by extension branch");
		conceptService.createUpdateConcept(concept, "MAIN/EXTENSION-A");

		// Check that the concept is saved in the extension branch index and versions replaced are recorded
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A");
		assertEquals("Updated by extension branch", concept.getTerm());
		Map<String, Set<String>> versionsReplaced =  branchService.findLatest("MAIN/EXTENSION-A").getVersionsReplaced();
		assertNotNull(versionsReplaced.get("Concept"));
		assertEquals(1, versionsReplaced.get("Concept").size());

		// Update metadata to exclude Concept from parent branch
		Map<String, Object> metadata = new HashMap<>();
		metadata.put(VersionControlHelper.PARENT_BRANCHES_EXCLUDED_ENTITY_CLASS_NAMES, List.of("Concept"));
		branchService.updateMetadata("MAIN/EXTENSION-A", metadata);


		// Update the concept in the extension branch and save
		concept.setTerm("Updated by extension branch again");
		conceptService.createUpdateConcept(concept, "MAIN/EXTENSION-A");

		// Check that the concept is saved in the extension branch and no versions replaced is recorded
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A");
		assertEquals("Updated by extension branch again", concept.getTerm());
		versionsReplaced =  branchService.findLatest("MAIN/EXTENSION-A").getVersionsReplaced();
		assertEquals(0, versionsReplaced.get("Concept").size());

		// Check project version
		branchService.create("MAIN/EXTENSION-A/PROJECT-A");
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A/PROJECT-A");
		assertEquals("Updated by extension branch again", concept.getTerm());

		// Check task version
		branchService.create("MAIN/EXTENSION-A/PROJECT-A/TASK-A");
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A/PROJECT-A");
		assertEquals("Updated by extension branch again", concept.getTerm());

		// Update config at project level to exclude Concept from parent branch which means it should not be visible in the project branch
		metadata.put(VersionControlHelper.PARENT_BRANCHES_EXCLUDED_ENTITY_CLASS_NAMES, List.of("Concept"));
		branchService.updateMetadata("MAIN/EXTENSION-A/PROJECT-A", metadata);
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A/PROJECT-A");
		assertNull(concept);

		// Not visible from task branch either
		concept = conceptService.findConcept("1", "MAIN/EXTENSION-A/PROJECT-A/TASK-A");
		assertNull(concept);
	}

	@Test
	void testMultiBranchCriteria() {
		// Test that version replaced works correctly in MultiBranchCriteria
		// Scenario: MAIN/B depends on MAIN/A via ADDITIONAL_DEPENDENT_BRANCHES
		
		// Create MAIN with a concept
		branchService.create("MAIN");
		conceptService.createUpdateConcept(new Concept("1", "Concept on MAIN"), "MAIN");
		
		// Create MAIN/A and update the concept (creates version replacement)
		branchService.create("MAIN/A");
		Concept concept = conceptService.findConcept("1", "MAIN/A");
		assertNotNull(concept, "MAIN/A should see MAIN's content on creation");
		assertEquals("Concept on MAIN", concept.getTerm(), "MAIN/A initially sees MAIN version");
		
		concept.setTerm("Updated in MAIN/A");
		conceptService.createUpdateConcept(concept, "MAIN/A");
		
		// Verify MAIN/A now has its own version with version replacement recorded
		assertEquals("Updated in MAIN/A", conceptService.findConcept("1", "MAIN/A").getTerm());
		Map<String, Set<String>> versionsReplacedA = branchService.findLatest("MAIN/A").getVersionsReplaced();
		assertNotNull(versionsReplacedA.get("Concept"), "MAIN/A should have version replacements");
		assertEquals(1, versionsReplacedA.get("Concept").size(), "MAIN/A replaced 1 version from MAIN");

		conceptService.createUpdateConcept(new Concept("2", "Concept 2 on MAIN/A"), "MAIN/A");
		
		// Create MAIN/B
		branchService.create("MAIN/B");
		
		// Before dependency: MAIN/B sees only MAIN version
		assertEquals("Concept on MAIN", conceptService.findConcept("1", "MAIN/B").getTerm());
		
		// Configure MAIN/B to depend on MAIN/A (triggers MultiBranchCriteria)
		Map<String, Object> metadata = new HashMap<>();
		metadata.put(VersionControlHelper.ADDITIONAL_DEPENDENT_BRANCHES, List.of("MAIN/A"));
		branchService.updateMetadata("MAIN/B", metadata);
		
		// After dependency: MAIN/B should see MAIN/A's version
		Concept result = conceptService.findConcept("1", "MAIN/B");
		assertNotNull(result);
		assertEquals("Updated in MAIN/A", result.getTerm(), 
				"Should see MAIN/A's version (MAIN version excluded by version replaced)");
		assertEquals("MAIN/A", result.getPath());

		// Concept 2 from MAIN/A should be visible via the dependency
		result = conceptService.findConcept("2", "MAIN/B");
		assertNotNull(result);
		assertEquals("MAIN/A", result.getPath(), "Concept 2 comes from MAIN/A (the dependency)");

		// Now update concept 2 on MAIN/B - this creates a version replacement
		concept = conceptService.findConcept("2", "MAIN/B");
		concept.setTerm("Updated in MAIN/B");
		conceptService.createUpdateConcept(concept, "MAIN/B");

		// After updating, concept 2 should now come from MAIN/B
		result = conceptService.findConcept("2", "MAIN/B");
		assertNotNull(result);
		assertEquals("MAIN/B", result.getPath(), "Concept 2 now comes from MAIN/B after local update");
	}


	@Test
	void testMultiBranchCriteriaWithOverrides() {
		// Create MAIN with a concept
		branchService.create("MAIN");
		conceptService.createUpdateConcept(new Concept("1", "Concept on MAIN"), "MAIN");

		// Create MAIN/A and update the concept (creates version replacement)
		branchService.create("MAIN/A");
		Concept concept = conceptService.findConcept("1", "MAIN/A");
		assertNotNull(concept, "MAIN/A should see MAIN's content on creation");
		assertEquals("Concept on MAIN", concept.getTerm(), "MAIN/A initially sees MAIN version");

		concept.setTerm("Updated in MAIN/A");
		conceptService.createUpdateConcept(concept, "MAIN/A");

		// Verify MAIN/A now has its own version with version replacement recorded
		assertEquals("Updated in MAIN/A", conceptService.findConcept("1", "MAIN/A").getTerm());
		Map<String, Set<String>> versionsReplacedA = branchService.findLatest("MAIN/A").getVersionsReplaced();
		assertNotNull(versionsReplacedA.get("Concept"), "MAIN/A should have version replacements");
		assertEquals(1, versionsReplacedA.get("Concept").size(), "MAIN/A replaced 1 version from MAIN");

		// Create MAIN/B
		branchService.create("MAIN/B");

		// Configure MAIN/B to depend on MAIN/A (triggers MultiBranchCriteria)
		Map<String, Object> metadata = new HashMap<>();
		metadata.put(VersionControlHelper.ADDITIONAL_DEPENDENT_BRANCHES, List.of("MAIN/A"));
		branchService.updateMetadata("MAIN/B", metadata);

		// It should see the version from MAIN/A
		Concept result = conceptService.findConcept("1", "MAIN/B");
		assertNotNull(result);
		assertEquals("Updated in MAIN/A", result.getTerm(),
				"Should see MAIN/A's version (MAIN version excluded by version replaced)");
		assertEquals("MAIN/A", result.getPath());

		// Updates 1 on MAIN/B
		concept.setTerm("Updated in MAIN/B");
		conceptService.createUpdateConcept(concept, "MAIN/B");
		// Verify version should be on MAIN/B
		concept = conceptService.findConcept("1", "MAIN/B");
		assertEquals("Updated in MAIN/B", concept.getTerm());

		Branch taskA = branchService.create("MAIN/B/TASKA");
		result = conceptService.findConcept("1", taskA.getPath());
		assertNotNull(result);
		assertEquals("Updated in MAIN/B", result.getTerm(),
				"Should see MAIN/B's version (MAIN/A version excluded by version replaced)");
		assertEquals("MAIN/B", result.getPath());

	}

	@AfterEach
	void tearDown() {
		branchService.deleteAll();
		conceptService.deleteAll();
	}
}

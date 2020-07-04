package io.kaicode.elasticvc;

import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.example.domain.Concept;
import io.kaicode.elasticvc.example.service.ConceptService;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

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

		Assert.assertNull("Domain entity is not created yet.", conceptService.findConcept("1", branch));

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branch);

		Concept conceptFromRepository = conceptService.findConcept("1", branch);
		Assert.assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals("There should be no versions replaced on MAIN", Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"));

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branch);

		assertEquals("There should be no versions replaced on MAIN", Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"));
	}

	@Test
	void testCreateFindUpdateOnChildOfMainBranch() {
		String branch = "MAIN/A";
		branchService.create("MAIN");
		branchService.create(branch);

		Assert.assertNull("Domain entity is not created yet.", conceptService.findConcept("1", branch));

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branch);

		Concept conceptFromRepository = conceptService.findConcept("1", branch);
		Assert.assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals("There should be no versions replaced on MAIN/A", Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"));

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branch);

		assertEquals("There should be no versions replaced on MAIN/A because the concept does not exist on the parent branch.",
				Collections.emptySet(), branchService.findLatest(branch).getVersionsReplaced().get("Concept"));


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

		assertEquals("Branch MAIN/A should now record 1 version replaced because concept 2 exists on the parent branch." +
						"This allows the version control system to hide the version on MAIN when finding domain entities.",
				1, branchService.findLatest(branch).getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size());

		try (Commit commit = branchService.openPromotionCommit("MAIN", "MAIN/A")) {
			commit.markSuccessful();
		}

		assertEquals("Branch MAIN/A should now record 0 versions replaced because it's content changes have been promoted.",
				0, branchService.findLatest("MAIN/A").getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size());
		assertEquals("Branch MAIN always have 0 versions replaced because it has no parents so this is not needed during content selection.",
				0, branchService.findLatest("MAIN").getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size());

	}

	@Test
	void testCommitRollback() {
		String branchPath = "MAIN/A";
		branchService.create("MAIN");
		branchService.create(branchPath);

		Assert.assertNull("Domain entity is not created yet.", conceptService.findConcept("1", branchPath));

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createUpdateConcept(conceptBeforeSave, branchPath);

		Concept conceptFromRepository = conceptService.findConcept("1", branchPath);
		Assert.assertNotNull(conceptFromRepository);
		assertEquals("Concept 1", conceptFromRepository.getTerm());

		assertEquals("There should be no versions replaced on MAIN/A", Collections.emptySet(), branchService.findLatest(branchPath).getVersionsReplaced().get("Concept"));

		conceptFromRepository.setTerm("Updated");
		conceptService.createUpdateConcept(conceptFromRepository, branchPath);

		assertEquals("There should be no versions replaced on MAIN/A because the concept does not exist on the parent branch.",
				Collections.emptySet(), branchService.findLatest(branchPath).getVersionsReplaced().get("Concept"));


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

		assertEquals("Branch MAIN/A should now record 1 version replaced because concept 2 exists on the parent branch." +
						"This allows the version control system to hide the version on MAIN when finding domain entities.",
				1, branchService.findLatest(branchPath).getVersionsReplaced().getOrDefault("Concept", Collections.emptySet()).size());

		// Rollback commit on MAIN/A
		Branch branchVersion = branchService.findLatest(branchPath);
		branchService.rollbackCompletedCommit(branchVersion, Collections.singletonList(Concept.class));

		branchVersion = branchService.findLatest(branchPath);
		assertEquals("The head timepoint should now match the previous commit.", branchVersionBeforeUpdate.getHead(), branchVersion.getHead());
		assertEquals("There should now be no versions replaced on MAIN/A because the commit was rolled back.",
				Collections.emptySet(), branchVersion.getVersionsReplaced().get("Concept"));
	}

	@AfterEach
	void tearDown() {
		branchService.deleteAll();
		conceptService.deleteAll();
	}

}

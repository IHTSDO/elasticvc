package io.kaicode.elasticvc;

import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.kaicode.elasticvc.domain.Branch.BranchState.*;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class BranchServiceTest {

	@Autowired
	private BranchService branchService;

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	@Test
	public void testCreateFindBranches() {
		assertNull(branchService.findLatest("MAIN"));

		final Branch branch = branchService.create("MAIN");
		assertEquals(UP_TO_DATE, branch.getState());

		final Branch main = branchService.findLatest("MAIN");
		assertNotNull(main);
		assertNotNull(main.getInternalId());
		assertNotNull(main.getPath());
		assertNotNull(main.getBase());
		assertNotNull(main.getHead());
		assertEquals("MAIN", main.getPath());
		assertEquals(UP_TO_DATE, main.getState());

		assertNull(branchService.findLatest("MAIN/A"));
		branchService.create("MAIN/A");
		final Branch a = branchService.findLatest("MAIN/A");
		assertNotNull(a);
		assertEquals("MAIN/A", a.getPath());
		assertEquals(UP_TO_DATE, a.getState());

		assertNotNull(branchService.findLatest("MAIN"));

		try {
			branchService.create("123");
			fail("Expected an exception.");
		} catch (IllegalArgumentException e) {

		}
		try {
			branchService.create("MAIN.abc");
			fail("Expected an exception.");
		} catch (IllegalArgumentException e) {

		}
		branchService.create("MAIN/abc");
	}

	@Test
	public void testFindAll() {
		assertEquals(0, branchService.findAll().size());

		branchService.create("MAIN");
		assertEquals(1, branchService.findAll().size());

		branchService.create("MAIN/A");
		branchService.create("MAIN/A/AA");
		branchService.create("MAIN/C");
		branchService.create("MAIN/C/something");
		branchService.create("MAIN/C/something/thing");
		branchService.create("MAIN/B");

		final List<Branch> branches = branchService.findAll();
		assertEquals(7, branches.size());

		assertEquals("MAIN", branches.get(0).getPath());
		assertEquals("MAIN/A", branches.get(1).getPath());
		assertEquals("MAIN/C/something/thing", branches.get(6).getPath());
	}

	@Test
	public void testFindChildren() {
		assertEquals(0, branchService.findAll().size());

		branchService.create("MAIN");
		assertEquals(0, branchService.findChildren("MAIN").size());

		branchService.create("MAIN/A");
		branchService.create("MAIN/A/AA");
		branchService.create("MAIN/C");
		branchService.create("MAIN/C/something");
		branchService.create("MAIN/C/something/thing");
		branchService.create("MAIN/B");

		final List<Branch> mainChildren = branchService.findChildren("MAIN");
		assertEquals(6, mainChildren.size());

		assertEquals("MAIN/A", mainChildren.get(0).getPath());
		assertEquals("MAIN/C/something/thing", mainChildren.get(5).getPath());

		final List<Branch> cChildren = branchService.findChildren("MAIN/C");
		assertEquals(2, cChildren.size());
		assertEquals("MAIN/C/something", cChildren.get(0).getPath());
		assertEquals("MAIN/C/something/thing", cChildren.get(1).getPath());
	}

	@Test
	public void testBranchState() {
		elasticsearchTemplate.putMapping(Branch.class);
		Map<String, String> meta = new HashMap<>();
		meta.put("test", "123");
		branchService.create("MAIN", meta);

		Map mapping = elasticsearchTemplate.getMapping(Branch.class);
		System.out.println(mapping);

		Date mainACreationDate = branchService.create("MAIN/A").getCreation();
		branchService.create("MAIN/A/A1");
		branchService.create("MAIN/B");

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", UP_TO_DATE);
		assertBranchState("MAIN/A/A1", UP_TO_DATE);
		assertBranchState("MAIN/B", UP_TO_DATE);

		makeEmptyCommit("MAIN/A");
		assertEquals(mainACreationDate, branchService.findBranchOrThrow("MAIN/A").getCreation());

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", FORWARD);
		assertBranchState("MAIN/A/A1", BEHIND);
		assertBranchState("MAIN/B", UP_TO_DATE);

		makeEmptyCommit("MAIN");

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", DIVERGED);
		assertBranchState("MAIN/A/A1", BEHIND);
		assertBranchState("MAIN/B", BEHIND);

		assertNull(branchService.findBranchOrThrow("MAIN/A").getLastPromotion());

		try (Commit commit = branchService.openPromotionCommit("MAIN", "MAIN/A")) {
			commit.markSuccessful();
		}
		assertEquals(mainACreationDate, branchService.findBranchOrThrow("MAIN/A").getCreation());

		assertNotNull(branchService.findBranchOrThrow("MAIN/A").getLastPromotion());

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", UP_TO_DATE);
		assertBranchState("MAIN/A/A1", BEHIND);
		assertBranchState("MAIN/B", BEHIND);

		makeEmptyCommit("MAIN");

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", BEHIND);
		assertBranchState("MAIN/A/A1", BEHIND);
		assertBranchState("MAIN/B", BEHIND);

		try (Commit commit = branchService.openRebaseCommit("MAIN/A")) {
			commit.markSuccessful();
		}
		assertEquals(mainACreationDate, branchService.findBranchOrThrow("MAIN/A").getCreation());

		assertNotNull(branchService.findBranchOrThrow("MAIN/A").getLastPromotion());

		assertBranchState("MAIN", UP_TO_DATE);
		assertBranchState("MAIN/A", UP_TO_DATE);
		assertBranchState("MAIN/A/A1", BEHIND);
		assertBranchState("MAIN/B", BEHIND);

		makeEmptyCommit("MAIN/A");

		assertEquals(mainACreationDate, branchService.findBranchOrThrow("MAIN/A").getCreation());
		assertNotNull(branchService.findBranchOrThrow("MAIN/A").getLastPromotion());

		Branch main = branchService.findLatest("MAIN");
		assertNotNull(main.getMetadata());
		assertEquals(1, main.getMetadata().size());
		assertEquals("123", main.getMetadata().get("test"));
	}

	@Test
	public void testExists() {
		branchService.create("MAIN");
		branchService.create("MAIN/AA");

		assertTrue(branchService.exists("MAIN"));
		assertTrue(branchService.exists("MAIN/AA"));
		assertFalse(branchService.exists("THING"));
		assertFalse(branchService.exists("MAIN/B"));
		assertFalse(branchService.exists("MAIN/AA/B"));
	}

	@Test
	public void testMetadata() {
		Map<String, String> metadata = new HashMap<>();
		metadata.put("something", "123");
		metadata.put("something-else", "456");
		branchService.create("MAIN", metadata);

		Branch main = branchService.findLatest("MAIN");
		assertEquals(metadata, main.getMetadata());
	}

	@Test
	public void testLoadInheritedMetadata() {
		Map<String, String> metadataA = new HashMap<>();
		metadataA.put("A", "A1");
		metadataA.put("B", "B1");
		branchService.create("MAIN", metadataA);
		makeEmptyCommit("MAIN");

		branchService.create("MAIN/one");

		Map<String, String> metadataB = new HashMap<>();
		metadataB.put("B", "B2");
		metadataB.put("C", "C2");
		branchService.create("MAIN/one/two", metadataB);

		Map<String, String> mergedMetadata = new HashMap<>();
		mergedMetadata.put("A", "A1");
		mergedMetadata.put("B", "B2");
		mergedMetadata.put("C", "C2");

		assertEquals(metadataA, branchService.findBranchOrThrow("MAIN", true).getMetadata());
		assertEquals(metadataA, branchService.findBranchOrThrow("MAIN/one", true).getMetadata());
		assertEquals(mergedMetadata, branchService.findBranchOrThrow("MAIN/one/two", true).getMetadata());
	}

	private void assertBranchState(String path, Branch.BranchState status) {
		assertEquals(status, branchService.findLatest(path).getState());
	}

	private void makeEmptyCommit(String path) {
		try (Commit commit = branchService.openCommit(path, "Empty commit.")) {
			commit.markSuccessful();
		}
	}

	@After
	public void tearDown() {
		branchService.deleteAll();
	}
}

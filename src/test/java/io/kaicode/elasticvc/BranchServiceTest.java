package io.kaicode.elasticvc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.repositories.BranchRepository;
import org.assertj.core.util.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.text.SimpleDateFormat;
import java.util.*;

import static io.kaicode.elasticvc.domain.Branch.BranchState.*;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class BranchServiceTest extends AbstractTest {

	@Autowired
	private BranchService branchService;

	@Autowired
	private BranchRepository branchRepository;

	@Autowired
	private ObjectMapper objectMapper;

	private final SimpleDateFormat lockMetadataDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

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
		IndexOperations indexOps = elasticsearchRestTemplate.indexOps(Branch.class);
		indexOps.putMapping(indexOps.createMapping(Branch.class));
		Map<String, Object> meta = new HashMap<>();
		meta.put("test", "123");
		branchService.create("MAIN", meta);

		Map mapping = indexOps.getMapping();
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
		assertEquals("123", main.getMetadata().getString("test"));
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
		Map<String, Object> metadata = new HashMap<>();
		metadata.put("something", "123");
		metadata.put("something-else", "456");
		metadata.put("some.nested.value", "this");
		metadata.put("that", Maps.newHashMap("that", "true"));
		branchService.create("MAIN", metadata);
		Branch main = branchService.findLatest("MAIN");
		assertEquals(metadata, main.getMetadata().getAsMap());
	}

	@Test
	public void testVersionReplaced() {
		Map<String, Set<String>> versionReplaced = new HashMap<>();
		versionReplaced.put("Concept", new HashSet<>(Arrays.asList("123", "234")));
		Branch main = branchService.create("MAIN");
		main.setVersionsReplaced(versionReplaced);
		branchRepository.save(main);
		main = branchService.findLatest("MAIN");
		// Add this test as the Spring data ES returns as List instead of Set for the map value
		Set<String> replaced = main.getVersionsReplaced().get("Concept");
		assertEquals(2, replaced.size());
	}

	@Test
	public void testLoadInheritedMetadata() {
		Map<String, Object> metadataA = new HashMap<>();
		metadataA.put("A", "A1");
		metadataA.put("B", "B1");
		Map<String, String> internalValueMap = Maps.newHashMap("this", "true");
		internalValueMap.put("that", "true");
		metadataA.put("internal", internalValueMap);
		branchService.create("MAIN", metadataA);
		makeEmptyCommit("MAIN");

		branchService.create("MAIN/one");

		Map<String, Object> metadataB = new HashMap<>();
		metadataB.put("B", "B2");
		metadataB.put("C", "C2");
		internalValueMap = Maps.newHashMap("this", "false");
		internalValueMap.put("another", "taskOnly");
		metadataB.put("internal", internalValueMap);
		branchService.create("MAIN/one/two", metadataB);

		Map<String, Object> mergedMetadata = new HashMap<>();
		mergedMetadata.put("A", "A1");
		mergedMetadata.put("B", "B2");
		mergedMetadata.put("C", "C2");
		// value from parent
		internalValueMap.put("that", "true");
		mergedMetadata.put("internal", internalValueMap);
		assertEquals(metadataA, branchService.findBranchOrThrow("MAIN", true).getMetadata().getAsMap());
		// lock branch to add lock message metadata and make sure it is not inherited by child branch
		branchService.lockBranch("MAIN", getBranchLockMetadata("Classifying"));
		assertEquals(metadataA, branchService.findBranchOrThrow("MAIN/one", true).getMetadata().getAsMap());
		assertEquals(mergedMetadata, branchService.findBranchOrThrow("MAIN/one/two", true).getMetadata().getAsMap());
	}

	private void assertBranchState(String path, Branch.BranchState status) {
		assertEquals(status, branchService.findLatest(path).getState());
	}

	private void makeEmptyCommit(String path) {
		try (Commit commit = branchService.openCommit(path, "Empty commit.")) {
			commit.markSuccessful();
		}
	}

	private String getBranchLockMetadata(String description) {
		Map<String, Object> lockMeta = new HashMap<>();
		lockMeta.put("creationDate", lockMetadataDateFormat.format(new Date()));
		Map<String, Object> lockContext = new HashMap<>();
		lockContext.put("userId", "test");
		lockContext.put("description", description);
		lockMeta.put("context", lockContext);
		try {
			return "{object}|" + objectMapper.writeValueAsString(lockMeta);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialise branch lock metadata", e);
		}
	}

	@AfterEach
	public void tearDown() {
		branchService.deleteAll();
	}
}

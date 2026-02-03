package io.kaicode.elasticvc.domain;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BranchTest {

	@Test
	void testIsParent() {
		assertTrue(new Branch("MAIN").isParent(new Branch("MAIN/A")));
		assertTrue(new Branch("MAIN/A").isParent(new Branch("MAIN/A/B")));

		assertFalse(new Branch("MAIN").isParent(new Branch("MAIN")));
		assertFalse(new Branch("MAIN/A").isParent(new Branch("MAIN/A")));
		assertFalse(new Branch("MAIN").isParent(new Branch("MAIN/A/B")));
		assertFalse(new Branch("MAIN/A").isParent(new Branch("MAIN")));
		assertFalse(new Branch("MAIN/A").isParent(new Branch("MAIN/B")));
	}

}

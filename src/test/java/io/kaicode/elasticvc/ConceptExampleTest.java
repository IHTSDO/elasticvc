package io.kaicode.elasticvc;

import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.example.domain.Concept;
import io.kaicode.elasticvc.example.service.ConceptService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestConfiguration.class)
public class ConceptExampleTest {

	@Autowired
	private ConceptService conceptService;

	@Autowired
	private BranchService branchService;

	@Test(expected = IllegalArgumentException.class)
	public void testBranchDoesNotExist() {
		conceptService.findConcept("1", "MAIN");
	}

	@Test
	public void testCreateFind() {
		branchService.create("MAIN");

		Assert.assertNull("Domain entity is not created yet.", conceptService.findConcept("1", "MAIN"));

		// Create entity
		Concept conceptBeforeSave = new Concept("1", "Concept 1");
		conceptService.createConcept(conceptBeforeSave, "MAIN");

		Concept conceptFromRepository = conceptService.findConcept("1", "MAIN");
		Assert.assertNotNull(conceptFromRepository);
		Assert.assertEquals("Concept 1", conceptFromRepository.getTerm());
	}

}

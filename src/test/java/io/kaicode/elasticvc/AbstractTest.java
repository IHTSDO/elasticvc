package io.kaicode.elasticvc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Testcontainers;


@Testcontainers
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {TestConfiguration.class})
abstract class AbstractTest {
	@Autowired
	ElasticsearchRestTemplate elasticsearchRestTemplate;

	private static ElasticsearchContainer elasticsearchContainer = TestConfiguration.getElasticsearchContainerInstance();

	@BeforeAll
	static void setUp() {
		if (!elasticsearchContainer.isRunning()) {
			elasticsearchContainer.start();
		}
	}
}

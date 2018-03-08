package io.kaicode.elasticvc;

import io.kaicode.elasticvc.api.ComponentService;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.example.domain.Concept;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.rest.ElasticsearchRestClient;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class TestConfiguration {

	private static final String ELASTIC_SEARCH_VERSION = "6.0.1";

	private static EmbeddedElastic standaloneTestElasticsearchServer;

	@Bean
	public ElasticsearchTemplate elasticsearchTemplate() {

		// Create and start test server
		createStandaloneTestInstance();

		// Connect to standalone instance
		ElasticsearchRestClient client = new ElasticsearchRestClient(new HashMap<>(), "http://localhost:9935");
		return new ElasticsearchTemplate(client);
	}

	@Bean
	public ComponentService componentService() {
		ComponentService.initialiseIndexAndMappingForPersistentClasses(
				elasticsearchTemplate(),
				Branch.class,
				Concept.class
		);
		return new ComponentService();
	}

	/**
	 * Downloads Elasticsearch and starts a standalone instance from temp directory.
	 * The instance shuts itself down when this JVM closes.
	 */
	private void createStandaloneTestInstance() {
		try {
			standaloneTestElasticsearchServer = EmbeddedElastic.builder()
					.withElasticVersion(ELASTIC_SEARCH_VERSION)
					.withStartTimeout(30, TimeUnit.SECONDS)
					.withSetting(PopularProperties.CLUSTER_NAME, "integration-test-cluster")
					.withSetting(PopularProperties.HTTP_PORT, 9935)
					.build()
					.start();
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("Failed to start standalone test server.");
		}
		standaloneTestElasticsearchServer.deleteIndices();
	}

}

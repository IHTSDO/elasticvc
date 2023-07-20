package io.kaicode.elasticvc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import io.kaicode.elasticvc.api.ComponentService;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.example.domain.Concept;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.Arrays;
import java.util.Date;

@SpringBootApplication
@EnableElasticsearchRepositories(basePackages = "io.kaicode.elasticvc")
public class TestConfiguration extends ElasticsearchConfiguration {

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private ElasticsearchClient elasticsearchClient;

	private static final String ELASTIC_SEARCH_DOCKER = "docker.elastic.co/elasticsearch/elasticsearch:8.7.1";

	@Container
	private static final ElasticsearchContainer elasticsearchContainer;

	static {
		elasticsearchContainer = new SnowstormElasticsearchContainer();
		elasticsearchContainer.start();
	}

	@PostConstruct
	public void  initComponentService() {
		ComponentService.initialiseIndexAndMappingForPersistentClasses(
				true,
				elasticsearchOperations,
				Branch.class,
				Concept.class
		);
	}

	static ElasticsearchContainer getElasticsearchContainerInstance() {
		return elasticsearchContainer;
	}


	@Override
	public ClientConfiguration clientConfiguration() {
		return ClientConfiguration.builder()
				.connectedTo(elasticsearchContainer.getHttpHostAddress())
				.build();
	}

	public static class SnowstormElasticsearchContainer extends ElasticsearchContainer {
		public SnowstormElasticsearchContainer() {
			super(ELASTIC_SEARCH_DOCKER);
			// these are mapped ports used by the test container the actual ports used might be different
			this.addFixedExposedPort(9235, 9235);
			this.addFixedExposedPort(9330, 9330);
			addEnv("xpack.security.enabled", "false");
			this.addEnv("cluster.name", "integration-test-cluster");
		}
	}

	@Bean
	@Override
	public ElasticsearchCustomConversions elasticsearchCustomConversions() {
		return new ElasticsearchCustomConversions(
				Arrays.asList(new DateToLong(), new LongToDate()));
	}

	@WritingConverter
	static class DateToLong implements Converter<Date, Long> {

		@Override
		public Long convert(Date date) {
			return date.getTime();
		}
	}

	@ReadingConverter
	static class LongToDate implements Converter<Long, Date> {
		@Override
		public Date convert(Long dateInMillis) {
			return new Date(dateInMillis);
		}
	}
}

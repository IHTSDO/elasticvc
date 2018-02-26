package io.kaicode.elasticvc;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetAddress;

@SpringBootApplication
public class TestConfiguration {

	private static final String INTEGRATION_TEST_CLUSTER = "integration-test-cluster";
	private static final int PORT = 9930;

	@Bean
	public ElasticsearchTemplate elasticsearchTemplate() throws IOException, InterruptedException {
		String clusterName = INTEGRATION_TEST_CLUSTER;
		int port = PORT;

		// Create and start a clean standalone Elasticsearch test instance
		new ElasticsearchTestHelper().startStandaloneElasticsearchForTest(clusterName, port);

		// Connect to standalone instance
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		TransportClient client = new PreBuiltTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), port));
		return new ElasticsearchTemplate(client);
	}

}

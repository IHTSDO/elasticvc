package io.kaicode.elasticvc;

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import java.io.IOException;

public class ElasticsearchTestHelper {

	public static final String ELASTIC_SEARCH_VERSION = "5.5.0";

	/**
	 * Downloads Elasticsearch and starts a standalone instance from temp directory.
	 * The instance shuts itself down when this JVM closes.
	 * @param clusterName
	 * @param port
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void startStandaloneElasticsearchForTest(String clusterName, int port) throws IOException, InterruptedException {
		EmbeddedElastic.builder()
				.withElasticVersion(ELASTIC_SEARCH_VERSION)
				.withSetting(PopularProperties.CLUSTER_NAME, clusterName)
				.withSetting(PopularProperties.TRANSPORT_TCP_PORT, port)
				.build()
				.start()
				.deleteIndices();
	}
}

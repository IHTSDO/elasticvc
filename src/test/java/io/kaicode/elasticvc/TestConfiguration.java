package io.kaicode.elasticvc;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.example.domain.Concept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import javax.annotation.PostConstruct;
import java.net.UnknownHostException;

@SpringBootApplication
public class TestConfiguration {

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	private Logger logger = LoggerFactory.getLogger(getClass());

	@PostConstruct
	public void cleanUp() throws UnknownHostException {
		logger.info("Deleting all existing entities before tests start");
		elasticsearchTemplate.deleteIndex(Concept.class);
		elasticsearchTemplate.deleteIndex(Branch.class);
	}

}

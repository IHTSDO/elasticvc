package io.kaicode.elasticvc.example.domain;

import io.kaicode.elasticvc.domain.DomainEntity;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Setting;

// Example domain entity
@Document(indexName = "#{@indexNameProvider.indexName('concept')}")
@Setting(settingPath = "elasticsearch-test-settings.json")
public class Concept extends DomainEntity<Concept>{

	// Must have an identifier field with a name other than 'id'
	private String conceptId;

	private String term;

	public static final String FIELD_ID = "conceptId";

	public Concept() {
	}

	public Concept(String conceptId, String term) {
		this.conceptId = conceptId;
		this.term = term;
	}

	@Override
	public String getId() {
		return conceptId;
	}

	public String getConceptId() {
		return conceptId;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public String getTerm() {
		return term;
	}

	@Override
	public boolean isComponentChanged(Concept existingComponent) {
		return !term.equals(existingComponent.term);
	}
}

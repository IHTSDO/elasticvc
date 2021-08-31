package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.DomainEntity;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class BranchCriteria {

	private final BoolQueryBuilder branchCriteria;
	private final Map<String, Set<String>> allEntityVersionsReplaced;
	private final Date timepoint;

	BranchCriteria(BoolQueryBuilder branchCriteria, Map<String, Set<String>> allEntityVersionsReplaced, Date timepoint) {
		this.branchCriteria = branchCriteria;
		this.allEntityVersionsReplaced = allEntityVersionsReplaced;
		this.timepoint = timepoint;
	}

	public BoolQueryBuilder getEntityBranchCriteria(Class<? extends DomainEntity> entityClass) {
		if (allEntityVersionsReplaced != null && !allEntityVersionsReplaced.isEmpty()) {
			Set<String> values = allEntityVersionsReplaced.get(entityClass.getSimpleName());
			if (values != null && !values.isEmpty()) {
				return boolQuery()
						.must(branchCriteria)
						.mustNot(termsQuery("_id", values));
			}
		}
		return boolQuery().must(branchCriteria);
	}

	public Date getTimepoint() {
		return timepoint;
	}

	@Override
	public String toString() {
		return "BranchCriteria{" +
				"branchCriteria=" + branchCriteria +
				", allEntityVersionsReplaced=" + (allEntityVersionsReplaced != null ? allEntityVersionsReplaced.size() : 0) +
				'}';
	}
}

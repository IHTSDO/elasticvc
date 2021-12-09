package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.DomainEntity;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public class BranchCriteria {

	private final String branchPath;
	private final Date timepoint;
	private BoolQueryBuilder branchCriteria;
	private Map<String, Set<String>> allEntityVersionsReplaced;
	private List<String> excludeContentFromPath;

	BranchCriteria(String branchPath, Date timepoint) {
		this.branchPath = branchPath;
		this.timepoint = timepoint;
	}

	public BranchCriteria(String branchPath, BoolQueryBuilder branchCriteria, Map<String, Set<String>> allEntityVersionsReplaced, Date timepoint) {
		this(branchPath, timepoint);
		this.branchCriteria = branchCriteria;
		this.allEntityVersionsReplaced = allEntityVersionsReplaced;
	}

	public BoolQueryBuilder getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass) {
		BoolQueryBuilder boolQueryBuilder = boolQuery().must(branchCriteria);

		if (allEntityVersionsReplaced != null && !allEntityVersionsReplaced.isEmpty()) {
			Set<String> values = allEntityVersionsReplaced.get(entityClass.getSimpleName());
			if (values != null && !values.isEmpty()) {
				boolQueryBuilder.mustNot(termsQuery("_id", values));
			}
		}
		if (excludeContentFromPath != null && !excludeContentFromPath.isEmpty()) {
			boolQueryBuilder.mustNot(termsQuery("path", excludeContentFromPath));
		}
		return boolQueryBuilder;
	}

	public void excludeContentFromPath(String path) {
		if (excludeContentFromPath == null) {
			excludeContentFromPath = new ArrayList<>();
		}
		excludeContentFromPath.add(path);
	}

	public String getBranchPath() {
		return branchPath;
	}

	public Date getTimepoint() {
		return timepoint;
	}

	@Override
	public String toString() {
		return "BranchCriteria{" +
				"branchPath=" + branchPath +
				"branchCriteria=" + branchCriteria +
				", allEntityVersionsReplaced=" + (allEntityVersionsReplaced != null ? allEntityVersionsReplaced.size() : 0) +
				'}';
	}
}

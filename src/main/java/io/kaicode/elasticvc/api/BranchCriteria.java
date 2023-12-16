package io.kaicode.elasticvc.api;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import io.kaicode.elasticvc.domain.DomainEntity;

import java.util.*;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.*;
import static io.kaicode.elasticvc.helper.QueryHelper.termsQuery;

public class BranchCriteria {

	private final String branchPath;
	private final Date timepoint;
	private Query branchCriteria;
	private Map<String, Set<String>> allEntityVersionsReplaced;
	private List<String> excludeContentFromPath;

	BranchCriteria(String branchPath, Date timepoint) {
		this.branchPath = branchPath;
		this.timepoint = timepoint;
	}

	public BranchCriteria(String branchPath, Query branchCriteria, Map<String, Set<String>> allEntityVersionsReplaced, Date timepoint) {
		this(branchPath, timepoint);
		this.branchCriteria = branchCriteria;
		this.allEntityVersionsReplaced = allEntityVersionsReplaced;
	}


	public Query getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass) {
		BoolQuery.Builder builder = bool().must(branchCriteria);

		if (allEntityVersionsReplaced != null && !allEntityVersionsReplaced.isEmpty()) {
			Set<String> values = allEntityVersionsReplaced.get(entityClass.getSimpleName());
			if (values != null && !values.isEmpty()) {
				builder.mustNot(termsQuery("_id", values));
			}
		}
		if (excludeContentFromPath != null && !excludeContentFromPath.isEmpty()) {
			builder.mustNot(termsQuery("path", excludeContentFromPath));
		}
		return builder.build()._toQuery();
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

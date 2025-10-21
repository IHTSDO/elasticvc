package io.kaicode.elasticvc.api;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import io.kaicode.elasticvc.domain.DomainEntity;

import java.util.*;

import static io.kaicode.elasticvc.helper.QueryHelper.termQuery;
import static io.kaicode.elasticvc.helper.QueryHelper.termsQuery;


@SuppressWarnings("unused")
public class MultiBranchCriteria extends BranchCriteria {

	private List<BranchCriteria> branchCriteria;

	public MultiBranchCriteria(String branchPath, Date timepoint) {
		super(branchPath, timepoint);
		this.branchCriteria = new ArrayList<>();
	}

	public MultiBranchCriteria(String branchPath, Date timepoint, List<BranchCriteria> branchCriteria) {
		this(branchPath, timepoint);
		this.branchCriteria = branchCriteria;
	}

	public void add(BranchCriteria branchCriteria) {
		this.branchCriteria.add(branchCriteria);
	}

	@Override
	public Query getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass) {
		return getEntityBranchCriteria(entityClass, true);
	}

	@Override
	protected Query getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass, boolean applyVersionsReplaced) {
		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
		
		if (branchCriteria.isEmpty()) {
			return boolQueryBuilder.must(termQuery("path", "this-will-match-nothing")).build()._toQuery();
		}

		// Aggregate version replacements from all branches
		Set<String> aggregatedVersionsReplaced = applyVersionsReplaced ?
				aggregateVersionsReplaced(entityClass) : Collections.emptySet();

		// Add should clauses for each branch (without individual version replacements)
		for (BranchCriteria criterion : branchCriteria) {
			boolQueryBuilder.should(criterion.getEntityBranchCriteria(entityClass, false));
		}

		// Apply aggregated version replacements at the top level
		if (!aggregatedVersionsReplaced.isEmpty()) {
			boolQueryBuilder.mustNot(termsQuery("_id", aggregatedVersionsReplaced));
		}

		return boolQueryBuilder.build()._toQuery();
	}

	private Set<String> aggregateVersionsReplaced(Class<? extends DomainEntity<?>> entityClass) {
		Set<String> aggregated = new HashSet<>();
		for (BranchCriteria criterion : branchCriteria) {
			Map<String, Set<String>> allVersionsReplaced = criterion.getAllEntityVersionsReplaced();
			if (allVersionsReplaced != null) {
				Set<String> versionsReplaced = allVersionsReplaced.get(entityClass.getSimpleName());
				if (versionsReplaced != null) {
					aggregated.addAll(versionsReplaced);
				}
			}
		}
		return aggregated;
	}

	public List<BranchCriteria> getBranchCriteria() {
		return branchCriteria;
	}
}

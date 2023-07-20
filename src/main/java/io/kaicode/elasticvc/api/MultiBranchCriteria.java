package io.kaicode.elasticvc.api;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import io.kaicode.elasticvc.domain.DomainEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static io.kaicode.elasticvc.helper.QueryHelper.termQuery;


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
	public BoolQuery.Builder getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass) {
		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
		if (branchCriteria.isEmpty()) {
			// Force matching nothing
			boolQueryBuilder.must(termQuery("path", "this-will-match-nothing"));
		} else {
			for (BranchCriteria branchCriterion : branchCriteria) {
				boolQueryBuilder.should(branchCriterion.getEntityBranchCriteria(entityClass).build()._toQuery());
			}
		}
		return boolQueryBuilder;
	}

	public List<BranchCriteria> getBranchCriteria() {
		return branchCriteria;
	}
}

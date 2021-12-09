package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.DomainEntity;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

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
	public BoolQueryBuilder getEntityBranchCriteria(Class<? extends DomainEntity<?>> entityClass) {
		BoolQueryBuilder boolQueryBuilder = boolQuery();
		if (branchCriteria.isEmpty()) {
			// Force matching nothing
			boolQueryBuilder.must(termQuery("path", "this-will-match-nothing"));
		} else {
			for (BranchCriteria branchCriterion : branchCriteria) {
				boolQueryBuilder.should(branchCriterion.getEntityBranchCriteria(entityClass));
			}
		}
		return boolQueryBuilder;
	}

	public List<BranchCriteria> getBranchCriteria() {
		return branchCriteria;
	}
}

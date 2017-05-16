package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.Entity;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

import static io.kaicode.elasticvc.api.VersionControlHelper.ContentSelection.CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class VersionControlHelper {

	@Autowired
	private BranchService branchService;

	@Autowired
	private ElasticsearchOperations elasticsearchTemplate;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public QueryBuilder getBranchCriteria(String path) {
		return getBranchCriteria(getBranchOrThrow(path));
	}

	public QueryBuilder getBranchCriteria(Branch branch) {
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	public QueryBuilder getBranchCriteriaIncludingOpenCommit(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplaced(), ContentSelection.STANDARD_SELECTION, commit);
	}

	public QueryBuilder getChangesOnBranchCriteria(String path) {
		final Branch branch = getBranchOrThrow(path);
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.CHANGES_ON_THIS_BRANCH_ONLY, null);
	}

	public QueryBuilder getBranchCriteriaChangesWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplaced(), ContentSelection.CHANGES_IN_THIS_COMMIT_ONLY, commit);
	}

	public QueryBuilder getBranchCriteriaChangesAndDeletionsWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplaced(), CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY, commit);
	}

	public BoolQueryBuilder getUpdatesOnBranchDuringRangeCriteria(String path, Date start, Date end) {
		final Branch branch = getBranchOrThrow(path);
		return boolQuery()
				.must(termQuery("path", branch.getFlatPath()))
				.must(boolQuery()
						.should(rangeQuery("start").gte(start).lte(end))
						.should(rangeQuery("end").gte(start).lte(end))
				);
	}

	private Branch getBranchOrThrow(String path) {
		final Branch branch = branchService.findLatest(path);
		if (branch == null) {
			throw new IllegalArgumentException("Branch '" + path + "' does not exist.");
		}
		return branch;
	}

	private BoolQueryBuilder getBranchCriteria(Branch branch, Date timepoint, Set<String> versionsReplaced, ContentSelection contentSelection, Commit commit) {
		final BoolQueryBuilder boolQueryShouldClause = boolQuery();
		final BoolQueryBuilder branchCriteria =
				boolQuery().should(boolQueryShouldClause.must(termQuery("path", branch.getFlatPath())));

		switch (contentSelection) {
			case STANDARD_SELECTION:
				// On this branch and started not ended
				boolQueryShouldClause.must(rangeQuery("start").lte(timepoint))
						.mustNot(existsQuery("end"));
				// Or any parent branch within time constraints
				addParentCriteriaRecursively(branchCriteria, branch, versionsReplaced);
				break;

			case CHANGES_ON_THIS_BRANCH_ONLY:
				// On this branch and started not ended
				boolQueryShouldClause.must(rangeQuery("start").lte(timepoint))
						.mustNot(existsQuery("end"));
				break;

			case CHANGES_IN_THIS_COMMIT_ONLY:
				// On this branch and started at commit date, not ended
				boolQueryShouldClause.must(termQuery("start", timepoint))
						.mustNot(existsQuery("end"));
				break;

			case CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY:
				// On this branch and started at commit date, not ended
				boolQueryShouldClause.must(boolQuery()
						.should(termQuery("start", timepoint))
						.should(termQuery("end", timepoint)));

				if (commit != null && commit.isRebase()) {

					// A rebase commit also includes all the changes
					// between the previous and new base timepoints on all ancestor branches

					// Collect previous and new base timepoints on all ancestor branches
					List<BranchTimeRange> branchTimeRanges = new ArrayList<>();
					Date tempBase = commit.getRebasePreviousBase();
					String parentPath = branch.getFatPath();
					while ((parentPath = PathUtil.getParentPath(parentPath)) != null) {
						Branch latestVersionOfParent = branchService.findAtTimepointOrThrow(parentPath, commit.getTimepoint());
						branchTimeRanges.add(new BranchTimeRange(parentPath, tempBase, latestVersionOfParent.getHead()));

						Branch baseVersionOfParent = branchService.findAtTimepointOrThrow(parentPath, tempBase);
						tempBase = baseVersionOfParent.getBase();
					}

					// Add all branch time ranges to selection criteria
					for (BranchTimeRange branchTimeRange : branchTimeRanges) {
						branchCriteria.should(boolQuery()
								.must(termQuery("path", PathUtil.flaten(branchTimeRange.getPath())))
								.must(rangeQuery("start").gt(branchTimeRange.getStart()))
								.must(boolQuery()
										.should(boolQuery().mustNot(existsQuery("end")))
										.should(rangeQuery("end").lte(branchTimeRange.getEnd()))
								)
						);
					}
				}

				break;
		}
		return branchCriteria;
	}

	void addParentCriteriaRecursively(BoolQueryBuilder branchCriteria, Branch branch, Set<String> versionsReplaced) {
		String parentPath = PathUtil.getParentPath(branch.getFatPath());
		if (parentPath != null) {
			final Branch parentBranch = branchService.findAtTimepointOrThrow(parentPath, branch.getBase());
			versionsReplaced = new HashSet<>(versionsReplaced);
			versionsReplaced.addAll(parentBranch.getVersionsReplaced());
			final Date base = branch.getBase();
			branchCriteria.should(boolQuery()
					.must(termQuery("path", parentBranch.getFlatPath()))
					.must(rangeQuery("start").lte(base))
					.must(boolQuery()
							.should(boolQuery().mustNot(existsQuery("end")))
							.should(rangeQuery("end").gt(base)))
					.mustNot(termsQuery("_id", versionsReplaced))
			);
			addParentCriteriaRecursively(branchCriteria, parentBranch, versionsReplaced);
		}
	}

	<T extends Entity> void endOldVersions(Commit commit, String idField, Class<T> clazz, Collection<? extends Object> ids, ElasticsearchCrudRepository repository) {
		// End versions of the entity on this path by setting end date
		final NativeSearchQuery query = new NativeSearchQueryBuilder()
				.withQuery(
						new BoolQueryBuilder()
								.must(termQuery("path", commit.getFlatBranchPath()))
								.must(rangeQuery("start").lt(commit.getTimepoint()))
								.mustNot(existsQuery("end"))
				)
				.withFilter(
						new BoolQueryBuilder()
								.must(termsQuery(idField, ids))
				)
				.build();

		List<T> toSave = new ArrayList<>();
		try (final CloseableIterator<T> localVersionsToEnd = elasticsearchTemplate.stream(query, clazz)) {
			localVersionsToEnd.forEachRemaining(version -> {
				version.setEnd(commit.getTimepoint());
				toSave.add(version);
			});
		}
		if (!toSave.isEmpty()) {
			repository.save(toSave);
			logger.debug("Ended {} {} {}", toSave.size(), clazz.getSimpleName(), toSave.stream().map(Entity::getInternalId).collect(Collectors.toList()));
			toSave.clear();
		}

		// Hide versions of the entity on other paths from this branch
		final NativeSearchQuery query2 = new NativeSearchQueryBuilder()
				.withQuery(
						new BoolQueryBuilder()
								.must(getBranchCriteriaIncludingOpenCommit(commit))
								.must(rangeQuery("start").lt(commit.getTimepoint()))
				)
				.withFilter(
						new BoolQueryBuilder()
								.must(termsQuery(idField, ids))
				)
				.build();

		Set<String> versionsReplaced = new HashSet<>();
		try (final CloseableIterator<T> replacedVersions = elasticsearchTemplate.stream(query2, clazz)) {
			replacedVersions.forEachRemaining(version -> {
				versionsReplaced.add(version.getInternalId());
			});
		}
		commit.addVersionsReplaced(versionsReplaced);

		logger.debug("Replaced {} {} {}", versionsReplaced.size(), clazz.getSimpleName(), versionsReplaced);
	}

	void setEntityMeta(Entity entity, Commit commit) {
		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(commit, "Commit must not be null");
		doSetEntityMeta(commit, entity);
	}

	void setEntityMeta(Collection<? extends Entity> entities, Commit commit) {
		Assert.notNull(entities, "Entities must not be null");
		Assert.notNull(commit, "Commit must not be null");
		for (Entity entity : entities) {
			doSetEntityMeta(commit, entity);
		}
	}

	private void doSetEntityMeta(Commit commit, Entity entity) {
		entity.setPath(commit.getFlatBranchPath());
		entity.setStart(commit.getTimepoint());
		entity.setEnd(null);
		entity.clearInternalId();
	}

	enum ContentSelection {
		STANDARD_SELECTION,
		CHANGES_ON_THIS_BRANCH_ONLY,
		CHANGES_IN_THIS_COMMIT_ONLY,
		CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY
	}

	private static final class BranchTimeRange {

		private String path;
		private Date start;
		private Date end;

		public BranchTimeRange(String path, Date start, Date end) {
			this.path = path;
			this.start = start;
			this.end = end;
		}

		public String getPath() {
			return path;
		}

		public Date getStart() {
			return start;
		}

		public Date getEnd() {
			return end;
		}
	}
}

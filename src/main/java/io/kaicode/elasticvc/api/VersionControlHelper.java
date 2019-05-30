package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import io.kaicode.elasticvc.domain.Entity;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.repository.ElasticsearchCrudRepository;
import org.springframework.data.util.CloseableIterator;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static io.kaicode.elasticvc.api.VersionControlHelper.ContentSelection.CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY;
import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class VersionControlHelper {

	public static final PageRequest LARGE_PAGE = PageRequest.of(0, 10_000);
	@Autowired
	private BranchService branchService;

	@Autowired
	private ElasticsearchOperations elasticsearchTemplate;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public BranchCriteria getBranchCriteria(String path) {
		return getBranchCriteria(getBranchOrThrow(path));
	}

	public BranchCriteria getBranchCriteria(Branch branch) {
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	public BranchCriteria getBranchCriteriaAtBranchCreation(String path) {
		Branch branch = branchService.findFirstVersionOrThrow(path);
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	public BranchCriteria getBranchCriteriaBeforeOpenCommit(Commit commit) {
		Branch branch = commit.getBranch();
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION_BEFORE_THIS_COMMIT, commit);
	}

	public BranchCriteria getBranchCriteriaAtTimepoint(String path, Date timepoint) {
		Branch branch = branchService.findAtTimepointOrThrow(path, timepoint);
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	public BranchCriteria getBranchCriteriaIncludingOpenCommit(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.STANDARD_SELECTION, commit);
	}

	public BranchCriteria getChangesOnBranchCriteria(String path) {
		final Branch branch = getBranchOrThrow(path);
		return getChangesOnBranchCriteria(branch);
	}

	public BranchCriteria getChangesOnBranchCriteria(Branch branch) {
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.CHANGES_ON_THIS_BRANCH_ONLY, null);
	}

	public BranchCriteria getChangesOnBranchIncludingOpenCommit(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.CHANGES_ON_THIS_BRANCH_ONLY, commit);
	}

	public BranchCriteria getBranchCriteriaChangesWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.CHANGES_IN_THIS_COMMIT_ONLY, commit);
	}

	public BranchCriteria getBranchCriteriaChangesAndDeletionsWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY, commit);
	}

	public BranchCriteria getBranchCriteriaUnpromotedChangesAndDeletions(Branch branch) {
		return getBranchCriteria(branch, null, null, ContentSelection.UNPROMOTED_CHANGES_AND_DELETIONS_ON_THIS_BRANCH, null);
	}

	public BranchCriteria getBranchCriteriaUnpromotedChanges(Branch branch) {
		return getBranchCriteria(branch, null, null, ContentSelection.UNPROMOTED_CHANGES_ON_THIS_BRANCH, null);
	}

	public BoolQueryBuilder getUpdatesOnBranchDuringRangeCriteria(String path, Date start, Date end) {
		final Branch branch = getBranchOrThrow(path);
		return boolQuery()
				.must(termQuery("path", branch.getPath()))
				.must(boolQuery()
						.should(rangeQuery("start").gte(start.getTime()).lte(end.getTime()))
						.should(rangeQuery("end").gte(start.getTime()).lte(end.getTime()))
				);
	}

	public BoolQueryBuilder getUpdatesOnBranchOrAncestorsDuringRangeQuery(String path, Date start, Date end) {
		List<Branch> startTimeSlice = getTimeSlice(path, start);
		List<Branch> endTimeSlice = getTimeSlice(path, end);

		BoolQueryBuilder shouldsQuery = boolQuery();
		for (int i = 0; i < startTimeSlice.size(); i++) {
			Branch startBranchTimepoint = startTimeSlice.get(i);
			Branch endBranchTimepoint = endTimeSlice.get(i);
			if (!startBranchTimepoint.getHead().equals(endBranchTimepoint.getHead())) {
				Date startDate = startBranchTimepoint.getHead();
				Date endDate = endBranchTimepoint.getHead();
				if (startBranchTimepoint.getPath().equals(path)) {
					startDate = start;
					endDate = end;
				}
				shouldsQuery.should(
						boolQuery()
								.must(termQuery("path", startBranchTimepoint.getPath()))
								.must(boolQuery()
										.should(rangeQuery("start").gte(startDate).lte(endDate))
										.should(rangeQuery("end").gte(startDate).lte(endDate))
								)
						);
			}
		}
		return boolQuery().must(shouldsQuery);
	}

	private Branch getBranchOrThrow(String path) {
		final Branch branch = branchService.findLatest(path);
		if (branch == null) {
			throw new IllegalArgumentException("Branch '" + path + "' does not exist.");
		}
		return branch;
	}

	private BranchCriteria getBranchCriteria(Branch branch, Date timepoint, Map<String, Set<String>> versionsReplaced, ContentSelection contentSelection, Commit commit) {

		final BoolQueryBuilder branchCriteria = boolQuery();
		// When adding top level clauses to the branchCriteria BoolQueryBuilder we must either use all 'should' clauses or all 'must' clauses.
		// If any 'must' clauses are given then the 'should' clauses do not have to match in Elasticsearch.
		// We will use a 'should' clause to select content from each branch that can match (usually this branch and ancestors).

		final BoolQueryBuilder thisBranchShouldClause = boolQuery().must(termQuery("path", branch.getPath()));
		branchCriteria.should(thisBranchShouldClause);

		Map<String, Set<String>> allEntityVersionsReplaced = null;
		switch (contentSelection) {
			case STANDARD_SELECTION:
				// On this branch and started not ended
				thisBranchShouldClause.must(rangeQuery("start").lte(timepoint.getTime()));
				thisBranchShouldClause.mustNot(existsQuery("end"));
				// Or any parent branch within time constraints
				allEntityVersionsReplaced = addParentCriteriaRecursively(branchCriteria, branch, versionsReplaced);
				break;

			case STANDARD_SELECTION_BEFORE_THIS_COMMIT:
				// On this branch and started not ended
				thisBranchShouldClause.must(rangeQuery("start").lte(timepoint.getTime()));
				thisBranchShouldClause.must(
						boolQuery()
								.should(boolQuery().mustNot(existsQuery("end")))
								.should(termQuery("end", commit.getTimepoint().getTime()))
				);
				// Or any parent branch within time constraints
				allEntityVersionsReplaced = addParentCriteriaRecursively(branchCriteria, branch, versionsReplaced);
				break;

			case CHANGES_ON_THIS_BRANCH_ONLY:
				// On this branch and started not ended
				thisBranchShouldClause.must(rangeQuery("start").lte(timepoint.getTime()))
						.mustNot(existsQuery("end"));
				break;

			case CHANGES_IN_THIS_COMMIT_ONLY:
				// On this branch and started at commit date, not ended
				thisBranchShouldClause.must(termQuery("start", timepoint.getTime()))
						.mustNot(existsQuery("end"));
				break;

			case CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY:
				// On this branch and started at commit date, not ended
				thisBranchShouldClause.must(boolQuery()
						.should(termQuery("start", timepoint.getTime()))
						.should(termQuery("end", timepoint.getTime())));

				if (commit != null && commit.isRebase()) {

					// A rebase commit also includes all the changes
					// between the previous and new base timepoints on all ancestor branches

					// Collect previous and new base timepoints on all ancestor branches
					List<BranchTimeRange> branchTimeRanges = new ArrayList<>();
					Date tempBase = commit.getRebasePreviousBase();
					String parentPath = branch.getPath();
					while ((parentPath = PathUtil.getParentPath(parentPath)) != null) {
						Branch latestVersionOfParent = branchService.findAtTimepointOrThrow(parentPath, commit.getTimepoint());
						branchTimeRanges.add(new BranchTimeRange(parentPath, tempBase, latestVersionOfParent.getHead()));

						Branch baseVersionOfParent = branchService.findAtTimepointOrThrow(parentPath, tempBase);
						tempBase = baseVersionOfParent.getBase();
					}

					// Add all branch time ranges to selection criteria
					for (BranchTimeRange branchTimeRange : branchTimeRanges) {
						// Add other should clauses for other branches
						branchCriteria.should(boolQuery()
								.must(termQuery("path", branchTimeRange.getPath()))
								.must(rangeQuery("start").gt(branchTimeRange.getStart().getTime()))
								.must(boolQuery()
										.should(boolQuery().mustNot(existsQuery("end")))
										.should(rangeQuery("end").lte(branchTimeRange.getEnd().getTime()))
								)
						);
					}
				}

				break;

			case UNPROMOTED_CHANGES_AND_DELETIONS_ON_THIS_BRANCH: {
					Date startPoint = branch.getLastPromotion() != null ? branch.getLastPromotion() : branch.getCreation();
				thisBranchShouldClause.must(boolQuery()
							.should(rangeQuery("start").gte(startPoint))
							.should(rangeQuery("end").gte(startPoint)));
				}
				break;

			case UNPROMOTED_CHANGES_ON_THIS_BRANCH: {
					Date startPoint = branch.getLastPromotion() != null ? branch.getLastPromotion() : branch.getCreation();
				thisBranchShouldClause
							.must(rangeQuery("start").gte(startPoint))
							.mustNot(existsQuery("end"));
				}
				break;
		}
		// Nest branch criteria in a 'must' clause so its 'should' clauses are not ignored if 'must' clauses are added to the query builder.
		BoolQueryBuilder must = boolQuery().must(branchCriteria);
		return new BranchCriteria(must, allEntityVersionsReplaced);
	}

	private Map<String, Set<String>> addParentCriteriaRecursively(BoolQueryBuilder branchCriteria, Branch branch, Map<String, Set<String>> versionsReplaced) {
		String parentPath = PathUtil.getParentPath(branch.getPath());
		if (parentPath != null) {
			final Branch parentBranch = branchService.findAtTimepointOrThrow(parentPath, branch.getBase());
			versionsReplaced = MapUtil.addAll(versionsReplaced, new HashMap<>());
			MapUtil.addAll(parentBranch.getVersionsReplaced(), versionsReplaced);
			final Date base = branch.getBase();
			branchCriteria.should(boolQuery()
					.must(termQuery("path", parentBranch.getPath()))
					.must(rangeQuery("start").lte(base.getTime()))
					.must(boolQuery()
							.should(boolQuery().mustNot(existsQuery("end")))
							.should(rangeQuery("end").gt(base.getTime())))
			);
			return addParentCriteriaRecursively(branchCriteria, parentBranch, versionsReplaced);
		}
		return versionsReplaced;
	}

	public List<Branch> getTimeSlice(String branchPath, Date timepoint) {
		List<Branch> branches = new ArrayList<>();
		addBranchAndAncestors(branchPath, timepoint, branches);
		return branches;
	}

	private void addBranchAndAncestors(String branchPath, Date timepoint, List<Branch> branches) {
		Branch branch = branchService.findAtTimepointOrThrow(branchPath, timepoint);
		branches.add(branch);
		if (!PathUtil.isRoot(branchPath)) {
			String parentPath = PathUtil.getParentPath(branchPath);
			addBranchAndAncestors(parentPath, branch.getBase(), branches);
		}
	}

	<T extends DomainEntity> void endOldVersions(Commit commit, String idField, Class<T> entityClass, Collection<? extends Object> ids, ElasticsearchCrudRepository repository) {
		// End versions of the entity on this path by setting end date
		endOldVersionsOnThisBranch(entityClass, ids, idField, null, commit, repository);

		// Hide versions of the entity on other paths from this branch
		final NativeSearchQuery query2 = new NativeSearchQueryBuilder()
				.withQuery(
						new BoolQueryBuilder()
								.must(getBranchCriteriaIncludingOpenCommit(commit).getEntityBranchCriteria(entityClass))
								.must(rangeQuery("start").lt(commit.getTimepoint().getTime()))
								.mustNot(termQuery("path", commit.getBranch().getPath()))
				)
				.withFilter(
						new BoolQueryBuilder()
								.must(termsQuery(idField, ids))
				)
				.withFields("internalId")
				.withPageable(LARGE_PAGE)
				.build();

		Set<String> versionsReplaced = new HashSet<>();
		try (final CloseableIterator<T> replacedVersions = elasticsearchTemplate.stream(query2, entityClass)) {
			replacedVersions.forEachRemaining(version -> {
				versionsReplaced.add(version.getInternalId());
			});
		}
		commit.addVersionsReplaced(versionsReplaced, entityClass);

		logger.debug("Replaced {} {} {}", versionsReplaced.size(), entityClass.getSimpleName(), versionsReplaced);
	}

	public <T extends DomainEntity> void endAllVersionsOnThisBranch(Class<T> entityClass, @Nullable QueryBuilder selectionClause, Commit commit, ElasticsearchCrudRepository repository) {
		endOldVersionsOnThisBranch(entityClass, null, null, selectionClause, commit, repository);
	}

	public <T extends DomainEntity> void endOldVersionsOnThisBranch(Class<T> entityClass, Collection<?> ids, String idField, QueryBuilder selectionClause,
			Commit commit, ElasticsearchCrudRepository repository) {

		if (ids != null && ids.isEmpty()) {
			return;
		}

		BoolQueryBuilder filterBuilder = boolQuery();
		if (ids != null) {
			filterBuilder.must(termsQuery(idField, ids));
		}
		if (selectionClause != null) {
			filterBuilder.must(selectionClause);
		}

		final NativeSearchQuery query = new NativeSearchQueryBuilder()
				.withQuery(
						boolQuery()
								.must(termQuery("path", commit.getBranch().getPath()))
								.must(rangeQuery("start").lt(commit.getTimepoint().getTime()))
								.mustNot(existsQuery("end"))
				)
				.withFilter(filterBuilder)
				.withPageable(LARGE_PAGE)
				.build();

		List<T> toSave = new ArrayList<>();
		try (final CloseableIterator<T> localVersionsToEnd = elasticsearchTemplate.stream(query, entityClass)) {
			localVersionsToEnd.forEachRemaining(version -> {
				version.setEnd(commit.getTimepoint());
				toSave.add(version);
			});
		}
		if (!toSave.isEmpty()) {
			repository.saveAll(toSave);
			logger.debug("Ended {} {} {}", toSave.size(), entityClass.getSimpleName(), toSave.stream().map(Entity::getInternalId).collect(Collectors.toList()));
			toSave.clear();
		}
	}

	public Map<String, Set<String>> getAllVersionsReplaced(List<Branch> timeSlice) {
		Map<String, Set<String>> allVersionsReplaced = new HashMap<>();
		for (Branch branch : timeSlice) {
			Map<String, Set<String>> branchVersionsReplaced = branch.getVersionsReplaced();
			for (String type : branchVersionsReplaced.keySet()) {
				allVersionsReplaced.computeIfAbsent(type, t -> new HashSet<>()).addAll(branchVersionsReplaced.get(type));
			}
		}
		return allVersionsReplaced;
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
		entity.setPath(commit.getBranch().getPath());
		entity.setStart(commit.getTimepoint());
		entity.setEnd(null);
		entity.clearInternalId();
	}

	enum ContentSelection {
		STANDARD_SELECTION,
		STANDARD_SELECTION_BEFORE_THIS_COMMIT,
		CHANGES_ON_THIS_BRANCH_ONLY,
		CHANGES_IN_THIS_COMMIT_ONLY,
		CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY,
		UNPROMOTED_CHANGES_AND_DELETIONS_ON_THIS_BRANCH,
		UNPROMOTED_CHANGES_ON_THIS_BRANCH;
	}

	private static final class BranchTimeRange {

		private String path;
		private Date start;
		private Date end;

		BranchTimeRange(String path, Date start, Date end) {
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

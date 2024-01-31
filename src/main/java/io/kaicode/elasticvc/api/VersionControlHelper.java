package io.kaicode.elasticvc.api;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.google.common.collect.Lists;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import io.kaicode.elasticvc.domain.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.*;
import static co.elastic.clients.json.JsonData.*;
import static io.kaicode.elasticvc.api.VersionControlHelper.ContentSelection.CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY;
import static io.kaicode.elasticvc.helper.QueryHelper.*;

@Service
public class VersionControlHelper {

	public static final PageRequest LARGE_PAGE = PageRequest.of(0, 10_000);
	public static final String VC_SEPARATE_INDEX_ENTITY_CLASS_NAMES = "vc.separate-index.entity-class-names";

	@Autowired
	private BranchService branchService;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public BranchCriteria getBranchCriteria(String path) {
		return getBranchCriteria(getBranchOrThrow(path));
	}

	public BranchCriteria getBranchCriteria(Branch branch) {
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaAtBranchCreationTimepoint(String path) {
		Branch branch = branchService.findFirstVersionOrThrow(path);
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaForParentBranchAtBranchBaseTimepoint(String path) {
		if (PathUtil.isRoot(path)) {
			throw new IllegalArgumentException("Can not access the base timepoint of the root branch.");
		}
		final Branch branch = getBranchOrThrow(path);
		Branch parentBranch = getBranchOrThrow(PathUtil.getParentPath(path));
		return getBranchCriteria(parentBranch, branch.getBase(), parentBranch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaBeforeOpenCommit(Commit commit) {
		Branch branch = commit.getBranch();
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION_BEFORE_THIS_COMMIT, commit);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaAtTimepoint(String path, Date timepoint) {
		Branch branch = branchService.findAtTimepointOrThrow(path, timepoint);
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.STANDARD_SELECTION, null);
	}

	public BranchCriteria getBranchCriteriaIncludingOpenCommit(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.STANDARD_SELECTION, commit);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getChangesOnBranchCriteria(String path) {
		final Branch branch = getBranchOrThrow(path);
		return getChangesOnBranchCriteria(branch);
	}
	@SuppressWarnings("unused")
	public BranchCriteria getChangesOnBranchCriteria(Branch branch) {
		return getBranchCriteria(branch, branch.getHead(), branch.getVersionsReplaced(), ContentSelection.CHANGES_ON_THIS_BRANCH_ONLY, null);
	}
	@SuppressWarnings("unused")
	public BranchCriteria getChangesOnBranchIncludingOpenCommit(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.CHANGES_ON_THIS_BRANCH_ONLY, commit);
	}
	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaChangesWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), ContentSelection.CHANGES_IN_THIS_COMMIT_ONLY, commit);
	}
	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaChangesAndDeletionsWithinOpenCommitOnly(Commit commit) {
		return getBranchCriteria(commit.getBranch(), commit.getTimepoint(), commit.getEntityVersionsReplacedIncludingFromBranch(), CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY, commit);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaUnpromotedChangesAndDeletions(Branch branch) {
		return getBranchCriteria(branch, null, null, ContentSelection.UNPROMOTED_CHANGES_AND_DELETIONS_ON_THIS_BRANCH, null);
	}

	@SuppressWarnings("unused")
	public BranchCriteria getBranchCriteriaUnpromotedChanges(Branch branch) {
		return getBranchCriteria(branch, null, null, ContentSelection.UNPROMOTED_CHANGES_ON_THIS_BRANCH, null);
	}

	@SuppressWarnings("unused")
	public BoolQuery.Builder getUpdatesOnBranchDuringRangeCriteria(String path, Date start, Date end) {
		final Branch branch = getBranchOrThrow(path);
		return bool()
				.must(termQuery("path", branch.getPath()))
				.must(bool(bq -> bq.should(range(rq -> rq.field("start").gte(of(start.getTime())).lte(of(end.getTime()))))
									.should(range(rq -> rq.field("end").gte(of(start.getTime())).lte(of(end.getTime())))))
					);
	}

	@SuppressWarnings("unused")
	public BoolQuery.Builder getUpdatesOnBranchOrAncestorsDuringRangeQuery(String path, Date start, Date end) {
		List<Branch> startTimeSlice = getTimeSlice(path, start);
		List<Branch> endTimeSlice = getTimeSlice(path, end);

		BoolQuery.Builder shouldsQuery = bool();
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
				final long startTime = startDate.getTime();
				final long endTime = endDate.getTime();
				shouldsQuery.should(
						bool(b -> b
								.must(termQuery("path", startBranchTimepoint.getPath()))
								.must(bool(bq -> bq.should(range(rq -> rq.field("start").gte(of(startTime)).lte(of(endTime))))
										         .should(range(rq -> rq.field("end").gte(of(startTime)).lte(of(endTime)))))
								))
						);
			}
		}
		return bool().must(shouldsQuery.build()._toQuery());
	}

	@SuppressWarnings("unused")
	private Branch getBranchOrThrow(String path) {
		final Branch branch = branchService.findLatest(path);
		if (branch == null) {
			throw new IllegalArgumentException("Branch '" + path + "' does not exist.");
		}
		return branch;
	}

	private BranchCriteria getBranchCriteria(Branch branch, Date timepoint, Map<String, Set<String>> versionsReplaced, ContentSelection contentSelection, Commit commit) {
		// When adding top level clauses to the branchCriteria BoolQuery.Builder we must either use all 'should' clauses or all 'must' clauses.
		// If any 'must' clauses are given then the 'should' clauses do not have to match in Elasticsearch.
		// We will use a 'should' clause to select content from each branch that can match (usually this branch and ancestors).

		final BoolQuery.Builder branchQueryBuilder = bool();
		final BoolQuery.Builder thisBranchShouldClause = bool().must(termQuery("path", branch.getPath()));
		Map<String, Set<String>> allEntityVersionsReplaced = null;
		switch (contentSelection) {
			case STANDARD_SELECTION -> {
				// On this branch and started and (not ended or ended later)
				thisBranchShouldClause.must(range(rq -> rq.field("start").lte(of(timepoint.getTime()))));
				thisBranchShouldClause.must(bool(b -> b
						.should(bool(bq -> bq.mustNot(existsQuery("end"))))
						.should(range(rq -> rq.field("end").gt(of(timepoint.getTime()))))
				));
				// Or any parent branch within time constraints
				allEntityVersionsReplaced = addParentCriteriaRecursively(branchQueryBuilder, branch, versionsReplaced);
			}
			case STANDARD_SELECTION_BEFORE_THIS_COMMIT -> {
				// On this branch and started not ended
				thisBranchShouldClause.must(range(rq -> rq.field("start").lte(of(timepoint.getTime()))));
				thisBranchShouldClause.must(
						bool(b -> b
								.should(bool(bq -> bq.mustNot(existsQuery("end"))))
								.should(termQuery("end", commit.getTimepoint().getTime())))
				);
				// Or any parent branch within time constraints
				allEntityVersionsReplaced = addParentCriteriaRecursively(branchQueryBuilder, branch, versionsReplaced);
			}
			case CHANGES_ON_THIS_BRANCH_ONLY ->
				// On this branch and started not ended
					thisBranchShouldClause.must(range(rq -> rq.field("start").lte(of(timepoint.getTime()))))
							.mustNot(existsQuery("end"));
			case CHANGES_IN_THIS_COMMIT_ONLY ->
				// On this branch and started at commit date, not ended
					thisBranchShouldClause.must(termQuery("start", timepoint.getTime()))
							.mustNot(existsQuery("end"));
			case CHANGES_AND_DELETIONS_IN_THIS_COMMIT_ONLY -> {
				// Include versions just deleted in this commit, from any ancestor
				thisBranchShouldClause.must(bool(b -> b
						.should(termQuery("start", timepoint.getTime()))
						.should(termQuery("end", timepoint.getTime()))));
				// Include versions just deleted in this commit, from any ancestor
				branchQueryBuilder.should(termsQuery("_id", commit.getEntityVersionsReplaced().values().stream().flatMap(Collection::stream).collect(Collectors.toSet())));
				if (commit.isRebase()) {

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
						branchQueryBuilder.should(bool(b -> b
								.must(termQuery("path", branchTimeRange.path()))
								.must(range(rq -> rq.field("start").gt(of(branchTimeRange.start().getTime()))))
								.must(bool(bq -> bq
												.should(bool(sb -> sb.mustNot(exists(eq -> eq.field("end")))))
												.should(range(rq -> rq.field("end").lte(of(branchTimeRange.end().getTime()))))
										)
								)));
					}
				}
			}
			case UNPROMOTED_CHANGES_AND_DELETIONS_ON_THIS_BRANCH -> {
				Date startPoint = branch.getLastPromotion() != null ? branch.getLastPromotion() : branch.getCreation();
				thisBranchShouldClause.must(bool(b -> b
						.should(range(rq -> rq.field("start").gte(of(startPoint.getTime()))))
						.should(range(rq -> rq.field("end").gte(of(startPoint.getTime()))))));
				// Include versions deleted on this branch, from any ancestor
				branchQueryBuilder.should(termsQuery("_id", branch.getVersionsReplaced().values().stream().flatMap(Collection::stream).collect(Collectors.toSet())));
			}
			case UNPROMOTED_CHANGES_ON_THIS_BRANCH -> {
				Date startPoint = branch.getLastPromotion() != null ? branch.getLastPromotion() : branch.getCreation();
				thisBranchShouldClause
						.must(range(rq -> rq.field("start").gte(of(startPoint.getTime()))))
						.mustNot(existsQuery("end"));
			}
		}
		// Nest branch criteria in a 'must' clause so its 'should' clauses are not ignored if 'must' clauses are added to the query builder.
		branchQueryBuilder.should(thisBranchShouldClause.build()._toQuery());
		Query must = branchQueryBuilder.build()._toQuery();
		BranchCriteria branchCriteria =  new BranchCriteria(branch.getPath(), must, allEntityVersionsReplaced, timepoint);
		getEntityClassNamesWithSeparateIndex(branch).forEach(entityClassName -> branchCriteria.excludeEntityContentFromPaths(entityClassName, getParentPaths(branch.getPath())));
		return branchCriteria;
	}

	private List<String> getParentPaths(String path) {
		List<String> parents = new ArrayList<>();
		String parentPath = PathUtil.getParentPath(path);
		while (parentPath != null) {
			parents.add(parentPath);
			parentPath = PathUtil.getParentPath(parentPath);
		}
		return parents;
	}

	private Map<String, Set<String>> addParentCriteriaRecursively(BoolQuery.Builder branchCriteria, Branch branch, Map<String, Set<String>> versionsReplaced) {
		String parentPath = PathUtil.getParentPath(branch.getPath());
		if (parentPath != null) {
			final Branch parentBranch = branchService.findAtTimepointOrThrow(parentPath, branch.getBase());
			versionsReplaced = MapUtil.addAll(versionsReplaced, new HashMap<>());
			MapUtil.addAll(parentBranch.getVersionsReplaced(), versionsReplaced);
			final Date base = branch.getBase();
			branchCriteria.should(bool(b -> b
					.must(termQuery("path", parentBranch.getPath()))
					.must(range(rq -> rq.field("start").lte(of(base.getTime()))))
					.must(bool(bq -> bq
							.should(bool(sb -> sb.mustNot(existsQuery("end"))))
							.should(range(rq -> rq.field("end").gt(of(base.getTime()))))))
			));
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

	<T extends DomainEntity<?>> void endOldVersions(Commit commit, String idField, Class<T> entityClass, Collection<?> ids, ElasticsearchRepository<T, String> repository) {
		// End versions of the entity on this path by setting end date
		endOldVersionsOnThisBranch(entityClass, ids, idField, null, commit, repository);

		// Skip versions hiding if entity is stored in a separate index
		if (getEntityClassNamesWithSeparateIndex(commit.getBranch()).contains(entityClass.getSimpleName())) {
			 logger.debug("Skipping versions hiding for {} on branch {}", entityClass.getSimpleName(), commit.getBranch().getPath());
		} else {
			// Hide versions of the entity on other paths from this branch
			final NativeQuery query = new NativeQueryBuilder()
					.withQuery(bool(b -> b
							.must(getBranchCriteriaIncludingOpenCommit(commit).getEntityBranchCriteria(entityClass))
							.must(range(rq -> rq.field("start").lt(of(commit.getTimepoint().getTime()))))
							.mustNot(termQuery("path", commit.getBranch().getPath()))))
					.withFilter(bool(bf -> bf.must(termsQuery(idField, ids))))
					.withSourceFilter(new FetchSourceFilter(new String[]{"internalId"}, null))
					.withPageable(LARGE_PAGE)
					.build();

			Set<String> versionsReplaced = new HashSet<>();
			try (final SearchHitsIterator<T> replacedVersions = elasticsearchOperations.searchForStream(query, entityClass)) {
				replacedVersions.forEachRemaining(version -> versionsReplaced.add(version.getContent().getInternalId()));
			}
			commit.addVersionsReplaced(versionsReplaced, entityClass);

			logger.debug("Replaced {} {} {}", versionsReplaced.size(), entityClass.getSimpleName(), versionsReplaced);
		}
	}

	public Collection<String> getEntityClassNamesWithSeparateIndex(Branch branch) {
		Map<String, Object> metaData = branch.getMetadata().getAsMap();
		if (metaData.containsKey(VC_SEPARATE_INDEX_ENTITY_CLASS_NAMES)) {
			Object value = metaData.get(VC_SEPARATE_INDEX_ENTITY_CLASS_NAMES);
			if (value instanceof String) {
				return List.of((String) value);
			} else if (value instanceof Collection) {
				return ((Collection<?>) value).stream().map(Object::toString).collect(Collectors.toList());
			}
		}
		return Collections.emptyList();
	}

	@SuppressWarnings("unused")
	public <T extends DomainEntity<?>> void endAllVersionsOnThisBranch(Class<T> entityClass, @Nullable Query selectionClause, Commit commit, ElasticsearchRepository<T, String> repository) {
		endOldVersionsOnThisBranch(entityClass, null, null, selectionClause, commit, repository);
	}

	public <T extends DomainEntity<?>> void endOldVersionsOnThisBranch(Class<T> entityClass, Collection<?> ids, String idField, Query selectionClause,
			Commit commit, ElasticsearchRepository<T, String> repository) {

		if (ids != null && ids.isEmpty()) {
			return;
		}

		BoolQuery.Builder filterBuilder = bool();
		if (ids != null) {
			filterBuilder.must(termsQuery(idField, ids));
		}
		if (selectionClause != null) {
			filterBuilder.must(selectionClause);
		}

		final NativeQuery query = new NativeQueryBuilder()
				.withQuery(
						bool(b -> b
								.must(termQuery("path", commit.getBranch().getPath()))
								.must(range(rq -> rq.field("start").lt(of(commit.getTimepoint().getTime()))))
								.mustNot(existsQuery("end"))))
				.withFilter(filterBuilder.build()._toQuery())
				.withPageable(LARGE_PAGE)
				.build();

		List<T> toSave = new ArrayList<>();
		try (final SearchHitsIterator<T> localVersionsToEnd = elasticsearchOperations.searchForStream(query, entityClass)) {
			localVersionsToEnd.forEachRemaining(version -> {
				version.getContent().setEnd(commit.getTimepoint());
				toSave.add(version.getContent());
			});
		}
		if (!toSave.isEmpty()) {
			for (List<T> partition : Lists.partition(toSave, 5_000)) {
				repository.saveAll(partition);
			}
			logger.debug("Ended {} {} {}", toSave.size(), entityClass.getSimpleName(), toSave.stream().map(Entity::getInternalId).collect(Collectors.toList()));
			toSave.clear();
		}
	}

	@SuppressWarnings("unused")
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

	@SuppressWarnings("unused")
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
		UNPROMOTED_CHANGES_ON_THIS_BRANCH
	}

	private record BranchTimeRange(String path, Date start, Date end) {}
}

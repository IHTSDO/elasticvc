package io.kaicode.elasticvc.api;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.domain.DomainEntity;
import io.kaicode.elasticvc.domain.Metadata;
import io.kaicode.elasticvc.repositories.BranchRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.*;
import static co.elastic.clients.json.JsonData.*;
import static io.kaicode.elasticvc.api.VersionControlHelper.LARGE_PAGE;
import static io.kaicode.elasticvc.helper.QueryHelper.*;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.elasticsearch.core.query.ScriptType.INLINE;

@Service
public class BranchService {

	public static final String LOCK_METADATA_KEY = "lock";

	@Autowired
	private BranchRepository branchRepository;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private VersionControlHelper versionControlHelper;

	private final BranchMetadataHelper branchMetadataHelper;

	private final List<CommitListener> commitListeners;

	private final Object branchLockSyncObject = new Object();

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public BranchService(@Autowired ObjectMapper objectMapper) {
		commitListeners = new ArrayList<>();
		branchMetadataHelper = new BranchMetadataHelper(objectMapper);
	}

	public Branch create(String path) {
		return create(path, null);
	}

	public Branch create(String path, Map<String, Object> metadataMap) {
		return doCreate(path, false, new Date(), new Metadata(metadataMap), null);
	}

	@SuppressWarnings("unused")
	public void createAtBaseTimepoint(String path, Date specificBaseTimepoint) {
		doCreate(path, false, new Date(), null, specificBaseTimepoint);
	}

	@SuppressWarnings("unused")
	public Branch recursiveCreate(String path) {
		return doCreate(path, true, new Date(), null, null);
	}

	private Branch doCreate(String path, boolean recursive, Date commitTimepoint, Metadata metadata, Date specificBaseTimepoint) {
		if (metadata == null) {
			metadata = new Metadata();
		}
		Assert.notNull(path, "Branch path can not be null.");
		Assert.isTrue(!path.contains("_"), "Branch path may not contain the underscore character: " + path);
		Assert.isTrue(path.startsWith("MAIN"), "The root branch must be 'MAIN'.");
		Assert.isTrue(path.equals("MAIN") || path.startsWith("MAIN/"), "Branch paths start with 'MAIN' and are separated by forward slash. For example 'MAIN/projectA'.");

		logger.debug("Attempting to create branch {}", path);
		if (exists(path)) {
			throw new IllegalArgumentException("Branch '" + path + "' already exists.");
		}
		final String parentPath = PathUtil.getParentPath(path);
		Branch parentBranch = null;
		if (parentPath != null) {
			parentBranch = findLatest(parentPath);
			if (parentBranch == null) {
				if (recursive) {
					doCreate(parentPath, true, commitTimepoint, null, specificBaseTimepoint);
				} else {
					throw new IllegalStateException("Parent branch '" + parentPath + "' does not exist.");
				}
			}
			logger.debug("Parent branch {}", parentBranch);
		}

		Branch branch = new Branch(path);
		Date base = parentBranch == null ? commitTimepoint : parentBranch.getHead();
		if (specificBaseTimepoint != null) {
			base = specificBaseTimepoint;
		}
		branch.setBase(base);
		branch.setHead(commitTimepoint);
		branch.setStart(commitTimepoint);
		branch.setMetadata(metadata);
		branch.setCreation(commitTimepoint);
		logger.info("Creating branch {}", branch);
		return save(branch).setState(Branch.BranchState.UP_TO_DATE);
	}

	private void updateInternalMetadata(Branch branch) {
		if (branch.getMetadata() == null) {
			branch.setMetadata(new Metadata());
		}
		final Map<String, String> flatMetadata = branchMetadataHelper.flattenObjectValues(branch.getMetadata().getAsMap());
		branch.setMetadataInternal(flatMetadata);
	}

	public boolean exists(String path) {
		return elasticsearchOperations.count(getBranchQuery(path, false), Branch.class) > 0;
	}

	public void deleteAll() {
		branchRepository.deleteAll();
	}

	public Branch findLatest(String path) {
		NativeQuery query = getBranchQuery(path, true);
		SearchHits<Branch> results = elasticsearchOperations.search(query, Branch.class);
		final List<Branch> branches = results.stream().map(SearchHit::getContent).toList();
		Branch branch = null;
		Branch parentBranch = null;

		for (Branch b : branches) {
			if (b.getPath().equals(path)) {
				if (branch != null) {
					return illegalState("There should not be more than one version of branch " + path + " with no end date.");
				}
				branch = b;
			} else {
				parentBranch = b;
			}
		}

		if (branch == null) {
			return null;
		}
		updatePublicMetadata(branch);

		if (!path.contains(PathUtil.SEPARATOR)) {
			// Root branch is always up to date
			return branch.setState(Branch.BranchState.UP_TO_DATE);
		}

		if (parentBranch == null) {
			return illegalState("Parent branch of " + path + " not found.");
		}
		branch.updateState(parentBranch.getHead());
		return branch;
	}

	private NativeQuery getBranchQuery(String path, boolean includeParent) {
		Assert.notNull(path, "The path argument is required, it must not be null.");

		final BoolQuery.Builder pathClauses = bool().should(termQuery("path", path));
		if (includeParent && path.contains(PathUtil.SEPARATOR)) {
			// Pick up the parent branch too
			pathClauses.should(termQuery("path", PathUtil.getParentPath(path)));
		}

		return new NativeQueryBuilder().withQuery(
				bool(bq -> bq.must(pathClauses.build()._toQuery())
						     .mustNot(existsQuery("end"))
				)).build();
	}

	public Branch findBranchOrThrow(String path) {
		return findBranchOrThrow(path, false);
	}

	public Branch findBranchOrThrow(String path, boolean includeInheritedMetadata) {
		final Branch branch = findLatest(path);
		if (branch == null) {
			throw new BranchNotFoundException("Branch '" + path + "' does not exist.");
		}
		if (includeInheritedMetadata) {
			String parentPath = PathUtil.getParentPath(branch.getPath());
			if (parentPath != null) {
				Branch parent = findBranchOrThrow(parentPath, true);
				if (parent.getMetadataInternal() != null) {
					final Metadata metadata = branch.getMetadata();
					final Metadata parentMetadata = parent.getMetadata();
					parentMetadata.getAsMap().forEach((key, value) -> {
						if (!LOCK_METADATA_KEY.equals(key) && !metadata.containsKey(key)) {
							metadata.getAsMap().put(key, value);
						}
					});
				}
			}
		}
		return branch;
	}

	private Branch updatePublicMetadata(Branch branch) {
		if (branch != null) {
			final Map<String, Object> publicMetadata = branchMetadataHelper.expandObjectValues(branch.getMetadataInternal());
			branch.setMetadata(new Metadata(publicMetadata));
		}
		return branch;
	}

	public Branch findFirstVersionOrThrow(String path) {
		List<Branch> branchVersions = elasticsearchOperations.search(
				new NativeQueryBuilder()
						.withQuery(termQuery("path", path))
						.withSort(s -> s.field(fb -> fb.field("start")))
						.withPageable(PageRequest.of(0, 1))
						.build(), Branch.class)
				.stream().map(SearchHit::getContent).toList();
		if (branchVersions.isEmpty()) {
			throw new BranchNotFoundException("Branch '" + path + "' does not exist.");
		}
		return updatePublicMetadata(branchVersions.iterator().next());
	}

	@SuppressWarnings("unused")
	public Page<Branch> findAllVersions(String path, Pageable pageable) {
		SearchHits<Branch> results = elasticsearchOperations.search(
				new NativeQueryBuilder()
						.withQuery(termQuery("path", path))
						.withSort(s -> s.field(fb -> fb.field("start")))
						.withPageable(pageable)
						.build(), Branch.class);

		return new PageImpl<>(results.get().map(SearchHit::getContent).map(this::updatePublicMetadata).collect(toList()), pageable, results.getTotalHits());
	}

	public Branch findAtTimepointOrThrow(String path, Date timepoint) {
		SearchHits<Branch> response = elasticsearchOperations.search(new NativeQueryBuilder()
				.withQuery(bool(b -> b
						.must(termQuery("path", path))
						.must(range(rq -> rq.field("start").lte(of(timepoint.getTime()))))
						.must(bool(bq -> bq
								.should(bool(eb -> eb.mustNot(existsQuery("end"))))
								.should(range(r -> r.field("end").gt(of(timepoint.getTime()))))))))
				.withSort(sb -> sb.field(f -> f.field("start")))
				.withPageable(PageRequest.of(0, 1))
				.build(), Branch.class, elasticsearchOperations.getIndexCoordinatesFor(Branch.class));

		final List<Branch> branches = new ArrayList<>();
		response.stream().forEach(r -> branches.add(r.getContent()));
		if (branches.isEmpty()) {
			SearchHits<Branch> mostRecentSearch = elasticsearchOperations.search(new NativeQueryBuilder()
					.withQuery(bool(b -> b.must(termQuery("path", path))))
					.withSort(sb -> sb.field(fb -> fb.field("start").order(SortOrder.Desc)))	// Descending
					.withPageable(PageRequest.of(0, 30))
					.build(), Branch.class, elasticsearchOperations.getIndexCoordinatesFor(Branch.class));
			final List<Branch> last30 = new ArrayList<>();
			mostRecentSearch.stream().forEach(result -> last30.add(result.getContent()));
			logger.info("Branch version missing. Logging last 30 versions:");
			for (Branch branchVersion : last30) {
				logger.info("- Branch version {}", branchVersion);
			}
			throw new IllegalStateException("Branch '" + path + "' does not exist at timepoint " + timepoint + " (" + timepoint.getTime() + ").");
		}

		final Branch branch = branches.get(0);
		updatePublicMetadata(branch);
		return branch;
	}

	public List<Branch> findAll() {
		SearchHits<Branch> searchHits = elasticsearchOperations.search(new NativeQueryBuilder()
				.withQuery(bool(b -> b.mustNot(eb -> eb.exists(ef -> ef.field("end")))))
				.withSort(sb -> sb.field(fb -> fb.field("path")))
				.withPageable(PageRequest.of(0, 10000))
				.build(), Branch.class);
		List<Branch> all = new ArrayList<>();
		searchHits.forEach(result -> all.add(result.getContent()));
		return all;
	}

	public List<Branch> findChildren(String path) {
		return findChildren(path, false); //All descendants by default
	}

	public List<Branch> findChildren(String path, boolean immediateChildren) {
		// mustNot(existsQuery("end")).must(prefixQuery("path", path + "/"))
		SearchHits<Branch> results = elasticsearchOperations.search(NativeQuery.builder()
				.withQuery(q -> q.bool(BoolQuery.of(b -> b.mustNot(QueryBuilders.exists().field("end").build()._toQuery())
						.must(prefix().field("path").value(path + "/").build()._toQuery()))))
				.withSort(sb -> sb.field(fb -> fb.field("path")))
				.build(), Branch.class);

		final List<Branch> children = new ArrayList<>();
		results.forEach(r -> children.add(updatePublicMetadata(r.getContent())));
		if (immediateChildren) {
			Branch parent = findBranchOrThrow(path);
			return children.stream()
					.filter(parent::isParent)
					.collect(toList());
		}
		return children;
	}

	@SuppressWarnings("unused")
	public boolean branchesHaveParentChildRelationship(Branch branchA, Branch branchB) {
		return branchA.isParent(branchB) || branchB.isParent(branchA);
	}

	public Commit openCommit(String path) {
		return openCommit(path, null);
	}

	public Commit openCommit(String path, String lockMetadata) {
		return openCommit(path, null, Commit.CommitType.CONTENT, null, lockMetadata);
	}

	private Commit openCommit(String branchPath, String mergeSourceBranchPath, Commit.CommitType commitType, String sourceBranchLockMetadata, String targetBranchLockMetadata) {
		synchronized (branchLockSyncObject) {
			Branch sourceBranch = null;
			if (commitType == Commit.CommitType.PROMOTION) {
				// Lock source branch as well as target
				sourceBranch = lockBranch(mergeSourceBranchPath, sourceBranchLockMetadata);
			}
			Branch branch = lockBranch(branchPath, targetBranchLockMetadata);
			Commit commit = new Commit(branch, commitType, this::completeCommit, this::rollbackCommit);
			if (commitType == Commit.CommitType.PROMOTION) {
				commit.setVersionsReplacedForPromotion(sourceBranch.getVersionsReplaced());
			}
			logger.info("Open commit on {} at {}", branchPath, commit.getTimepoint().getTime());
			return commit;
		}
	}

	public Branch lockBranch(String branchPath, String lockMetadata) {
		Branch branch = findBranchOrThrow(branchPath);
		return lockBranch(branch, lockMetadata);
	}

	private Branch lockBranch(Branch branch, String lockMetadataMessage) {
		if (branch.isLocked()) {
			throw new IllegalStateException(String.format("Branch %s is already locked", branch.getPath()));
		}
		branch.setLocked(true);
		branch.getMetadata().putString(LOCK_METADATA_KEY, lockMetadataMessage);
		return save(branch);
	}

	@SuppressWarnings("unused")
	public Branch updateMetadata(String path, Metadata metadata) {
		return updateMetadata(path, metadata.getAsMap());
	}

	public Branch updateMetadata(String path, Map<String, Object> metadataMap) {
		Branch branch = findBranchOrThrow(path);
		branch.setMetadata(new Metadata(metadataMap));
		return save(branch);
	}

	public Commit openRebaseCommit(String path) {
		return openRebaseCommit(path, null);
	}

	public Commit openRebaseCommit(String path, String lockMetadata) {
		return doOpenRebaseCommit(path, lockMetadata, null);
	}

	@SuppressWarnings("unused")
	public Commit openRebaseToSpecificParentTimepointCommit(String path, Date specificParentTimepoint, String lockMetadata) {
		return doOpenRebaseCommit(path, lockMetadata, specificParentTimepoint);
	}

	private Commit doOpenRebaseCommit(String path, String lockMetadata, Date specificParentTimepoint) {
		final Commit commit = openCommit(path, null, Commit.CommitType.REBASE, null, lockMetadata);
		final Branch branch = commit.getBranch();
		if (!PathUtil.isRoot(path)) {
			final String parentPath = PathUtil.getParentPath(path);
			final Branch parentBranch = findAtTimepointOrThrow(parentPath, specificParentTimepoint != null ? specificParentTimepoint : commit.getTimepoint());
			if (specificParentTimepoint != null && !parentBranch.getHead().equals(specificParentTimepoint)) {
				throw new IllegalStateException(String.format("Specific timepoint %s requested but not found for branch %s", specificParentTimepoint.getTime(), parentBranch));
			}
			commit.setRebasePreviousBase(branch.getBase());
			branch.setBase(parentBranch.getHead());
			commit.setSourceBranchPath(parentPath);
		}
		return commit;
	}

	public Commit openPromotionCommit(String path, String sourcePath) {
		return openPromotionCommit(path, sourcePath, null, null);
	}

	public Commit openPromotionCommit(String path, String sourcePath, String sourceBranchLockMetadata, String targetBranchLockMetadata) {
		final Commit commit = openCommit(path, sourcePath, Commit.CommitType.PROMOTION, sourceBranchLockMetadata, targetBranchLockMetadata);
		commit.setSourceBranchPath(sourcePath);
		return commit;
	}

	private void completeCommit(Commit commit) {
		try {
			for (CommitListener commitListener : commitListeners) {
				commitListener.preCommitCompletion(commit);
			}
		} catch (IllegalStateException | ElasticsearchException e) {
			logger.error("Commit commitListener threw {}, rolling back commit {} on branch {}",
					e.getClass().getSimpleName(), commit.getTimepoint().getTime(), commit.getBranch().getPath(), e);

			rollbackCommit(commit);
			throw e;
		}

		final Date timepoint = commit.getTimepoint();
		final Branch oldBranchTimespan = commit.getBranch();
		Date newBase = oldBranchTimespan.getBase();
		if (commit.isRebase()) {
			resetBranchBase(commit);
		}
		clearLock(oldBranchTimespan);
		oldBranchTimespan.setEnd(timepoint);

		final String path = oldBranchTimespan.getPath();
		final Branch newBranchTimespan = new Branch(path);
		newBranchTimespan.setBase(newBase);
		newBranchTimespan.setStart(timepoint);
		newBranchTimespan.setHead(timepoint);
		newBranchTimespan.setMetadata(oldBranchTimespan.getMetadata());

		// Clear previous versions replaced for entity classes that have separate documents (i.e not inherit from MAIN)
		Map<String, Set<String>> oldVersionsReplaced = oldBranchTimespan.getVersionsReplaced();
		versionControlHelper.getParentBranchesExcludedEntityClassNames(newBranchTimespan).forEach(oldVersionsReplaced::remove);
		newBranchTimespan.addVersionsReplaced(oldVersionsReplaced);

		newBranchTimespan.addVersionsReplaced(commit.getEntityVersionsReplaced());
		newBranchTimespan.setCreation(oldBranchTimespan.getCreation());
		newBranchTimespan.setLastPromotion(oldBranchTimespan.getLastPromotion());

		// Preserve existing commit versions replaced
		oldBranchTimespan.setVersionsReplaced(findBranchOrThrow(path).getVersionsReplaced());

		final List<Branch> newBranchVersionsToSave = new ArrayList<>();
		newBranchVersionsToSave.add(oldBranchTimespan);
		newBranchVersionsToSave.add(newBranchTimespan);

		final Commit.CommitType commitType = commit.getCommitType();
		newBranchTimespan.setContainsContent(commitType != Commit.CommitType.REBASE || oldBranchTimespan.isContainsContent());
		if (commitType == Commit.CommitType.PROMOTION) {
			final String sourceBranchPath = commit.getSourceBranchPath();
			if (Strings.isNullOrEmpty(sourceBranchPath)) {
				throw new IllegalArgumentException("The sourceBranchPath must be set for a commit of type " + Commit.CommitType.PROMOTION);
			}
			// Update source branch base to parent head
			// Clear versions replaced on source
			final Branch oldSourceBranch = findAtTimepointOrThrow(sourceBranchPath, timepoint);
			oldSourceBranch.setEnd(timepoint);
			clearLock(oldSourceBranch);
			if (!PathUtil.isRoot(path)) {
				newBranchTimespan.addVersionsReplaced(commit.getVersionsReplacedForPromotion());
			} else {
				// Root branch has no need for versions replaced collection.
				newBranchTimespan.getVersionsReplaced().clear();
			}
			newBranchVersionsToSave.add(oldSourceBranch);

			Branch newSourceBranch = new Branch(sourceBranchPath);
			newSourceBranch.setBase(timepoint);
			newSourceBranch.setStart(timepoint);
			newSourceBranch.setHead(timepoint);
			newSourceBranch.setMetadata(oldSourceBranch.getMetadata());
			newSourceBranch.setLastPromotion(timepoint);
			newSourceBranch.setCreation(oldSourceBranch.getCreation());
			newSourceBranch.setContainsContent(false);
			newBranchVersionsToSave.add(newSourceBranch);
			logger.debug("Updating branch base and clearing versionsReplaced {}", newSourceBranch);
		}

		logger.debug("Ending branch timespan {}", oldBranchTimespan);
		logger.debug("Starting branch timespan {}", newBranchTimespan);
		saveAll(newBranchVersionsToSave);
		logger.info("Completed commit on {} at {}", commit.getBranch().getPath(), commit.getTimepoint().getTime());
	}

	private Branch save(Branch branch) {
		updateInternalMetadata(branch);
		return branchRepository.save(branch);
	}

	private void saveAll(Iterable<Branch> branches) {
		branches.forEach(this::updateInternalMetadata);
		branchRepository.saveAll(branches);
	}

	private void rollbackCommit(Commit commit) {
		logger.info("Rolling back commit on {} started at {}", commit.getBranch().getPath(), commit.getTimepoint().getTime());

		@SuppressWarnings("unchecked")
		Set<Class<? extends DomainEntity<?>>> domainEntityClasses = commit.getDomainEntityClasses().stream().map(clazz -> (Class<? extends DomainEntity<?>>) clazz).collect(Collectors.toSet());
		doContentRollback(commit.getBranch().getPath(), commit.getSourceBranchPath(), commit.getTimepoint().getTime(), domainEntityClasses);

		Branch branch = commit.getBranch();
		if (commit.isRebase()) {
			resetBranchBase(commit);
		}

		if (commit.getCommitType() == Commit.CommitType.PROMOTION) {
			unlock(commit.getSourceBranchPath());
		}

		// Restore previous metadata
		final Branch latest = findLatest(branch.getPath());
		if (latest != null) {
			branch.setMetadata(latest.getMetadata());
		}
		clearLock(branch);
		save(branch);
	}

	public void rollbackCompletedCommit(Branch branchVersion, List<Class<? extends DomainEntity<?>>> domainTypes) {
		long timestamp = branchVersion.getHeadTimestamp();
		String path = branchVersion.getPath();
		boolean lockedInitially = branchVersion.isLocked();
		logger.info("Rolling back commit {} on {}.", timestamp, path);

		// Delete branch/commit then immediately lock the branch (again)
		Branch previousBranchVersion = findAtTimepointOrThrow(path, new Date(timestamp - 1));
		branchRepository.delete(branchVersion);
		previousBranchVersion.setEnd(null);
		previousBranchVersion.setLocked(false);
		// (Also saves the branch version)
		lockBranch(previousBranchVersion, getLockMessageOrNull(branchVersion));

		SearchHits<Branch> searchHits = elasticsearchOperations.search(new NativeQueryBuilder().withQuery(
				bq -> bq.bool(BoolQuery.of(b -> b
				.must(p -> p.prefix(pq -> pq.field(Branch.Fields.PATH).value(path + "/")))
				.must(termQuery(Branch.Fields.END, timestamp))))).build(), Branch.class);
		String promotionSourceBranch = null;
		if (!searchHits.isEmpty()) {
			promotionSourceBranch = searchHits.getSearchHit(0).getContent().getPath();
		}
		doContentRollback(path, promotionSourceBranch, timestamp, domainTypes);

		if (!lockedInitially) {
			unlock(path);
		}

		logger.info("Completed rollback of commit {} on {}.", timestamp, path);
	}

	private void doContentRollback(String path, String promotionSourceBranch, long timestamp, Collection<Class<? extends DomainEntity<?>>> domainTypes) {
		logger.info("Deleting documents on {} started at {}.", path, timestamp);
		Query deleteQuery = new NativeQueryBuilder()
				.withQuery(bool(b -> b
				.must(termQuery("path", path))
				.must(termQuery("start", timestamp)))).build();
		for (Class<? extends DomainEntity<?>> domainEntityClass : domainTypes) {
			elasticsearchOperations.delete(deleteQuery, domainEntityClass, elasticsearchOperations.getIndexCoordinatesFor(domainEntityClass));
			elasticsearchOperations.indexOps(domainEntityClass).refresh();
		}

		Set<String> branchPaths = new HashSet<>();
		branchPaths.add(path);
		if (promotionSourceBranch != null) {
			branchPaths.add(promotionSourceBranch);
		}
		logger.info("Clearing end time for documents on {} ended at {}.", branchPaths, timestamp);
		for (Class<? extends DomainEntity<?>> type : domainTypes) {
			// Find ended documents
			Set<String> endedDocumentIds = new HashSet<>();
			NativeQuery endedDocumentQuery = new NativeQueryBuilder()
					.withQuery(bool(b -> b
							.must(termQuery("end", timestamp))
							.must(termsQuery("path", branchPaths))))
					.withSourceFilter(new FetchSourceFilter(new String[]{"internalId"}, null))
					.withPageable(LARGE_PAGE).build();
			try (final SearchHitsIterator<? extends DomainEntity<?>> endedDocs = elasticsearchOperations.searchForStream(endedDocumentQuery, type)) {
				endedDocs.forEachRemaining(d -> endedDocumentIds.add(d.getContent().getInternalId()));
			}

			// Clear end dates
			List<UpdateQuery> updateQueries = new ArrayList<>();
			for (String internalId : endedDocumentIds) {
				UpdateQuery updateQuery = UpdateQuery.builder(internalId)
						.withScript("ctx._source.remove('end')")
						.withScriptType(INLINE)
						.withLang("painless")
						.build();
				updateQueries.add(updateQuery);
			}
			Iterators.partition(updateQueries.iterator(), 1_000).forEachRemaining(updateQueryBatch -> {
				if (!updateQueryBatch.isEmpty()) {
					elasticsearchOperations.bulkUpdate(updateQueryBatch, elasticsearchOperations.getIndexCoordinatesFor(type));
				}
			});
			elasticsearchOperations.indexOps(type).refresh();
			if (!endedDocumentIds.isEmpty()) {
				logger.info("{} ended documents restored for type {}.", endedDocumentIds.size(), type.getSimpleName());
			}
		}
	}

	public String getLockMessageOrNull(Branch branchVersion) {
		return branchVersion.getMetadata().getString(LOCK_METADATA_KEY);
	}

	private void resetBranchBase(Commit commit) {
		commit.getBranch().setBase(commit.getRebasePreviousBase());
	}

	public void unlock(String path) {
		final List<Branch> branches = elasticsearchOperations.search(new NativeQueryBuilder()
				.withQuery(bool(bq -> bq
						.must(termQuery("path", path))
						.mustNot(existsQuery("end"))))
				.withPageable(PageRequest.of(0, 1))
				.build(), Branch.class)
				.stream().map(SearchHit::getContent).toList();

		if (!branches.isEmpty()) {
			final Branch branch = branches.get(0);
			updatePublicMetadata(branch);
			clearLock(branch);
			save(branch);
		} else {
			throw new IllegalArgumentException("Branch not found " + path);
		}
	}

	@SuppressWarnings("unused")
	public void addCommitListener(CommitListener commitListener) {
		if (!commitListeners.contains(commitListener)) {
			commitListeners.add(commitListener);
		}
	}

	private Branch illegalState(String message) {
		logger.error(message);
		throw new IllegalStateException(message);
	}

	private void clearLock(Branch branchVersion) {
		branchVersion.setLocked(false);
		branchVersion.getMetadata().remove(LOCK_METADATA_KEY);
	}

	/**
	 * Get unmodifiable list of commit listeners. To add a listener use the addCommitListener method.
	 * @return unmodifiable list of commit listeners.
	 */
	@SuppressWarnings("unused")
	public List<CommitListener> getCommitListeners() {
		return Collections.unmodifiableList(commitListeners);
	}
}

package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.domain.Commit;
import io.kaicode.elasticvc.repositories.BranchRepository;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.DeleteQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class BranchService {

	public static final String LOCK_METADATA_KEY = "lock";

	@Autowired
	private BranchRepository branchRepository;

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;

	private final List<CommitListener> commitListeners;

	private final Integer branchLockSyncObject = 0;

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public BranchService() {
		commitListeners = new ArrayList<>();
	}

	public Branch create(String path) {
		return create(path, null);
	}

	public Branch create(String path, Map<String, String> metadata) {
		return doCreate(path, false, new Date(), metadata);
	}

	public Branch recursiveCreate(String path) {
		return doCreate(path, true, new Date(), null);
	}

	private Branch doCreate(String path, boolean recursive, Date commitTimepoint, Map<String, String> metadata) {
		Assert.notNull(path, "Branch path can not be null.");
		Assert.isTrue(!path.contains("_"), "Branch path may not contain the underscore character: " + path);

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
					doCreate(parentPath, true, commitTimepoint, null);
				} else {
					throw new IllegalStateException("Parent branch '" + parentPath + "' does not exist.");
				}
			}
			logger.debug("Parent branch {}", parentBranch);
		}

		Branch branch = new Branch(path);
		branch.setBase(parentBranch == null ? commitTimepoint : parentBranch.getHead());
		branch.setHead(commitTimepoint);
		branch.setStart(commitTimepoint);
		branch.setMetadataInternal(metadata);
		branch.setCreation(commitTimepoint);
		logger.info("Creating branch {}", branch);
		return branchRepository.save(branch).setState(Branch.BranchState.UP_TO_DATE);
	}

	public boolean exists(String path) {
		return elasticsearchTemplate.count(getBranchQuery(path, false), Branch.class) > 0;
	}

	public void deleteAll() {
		branchRepository.deleteAll();
	}

	public Branch findLatest(String path) {
		NativeSearchQuery query = getBranchQuery(path, true);
		final List<Branch> branches = elasticsearchTemplate.queryForList(query, Branch.class);

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

	private NativeSearchQuery getBranchQuery(String path, boolean includeParent) {
		Assert.notNull(path, "The path argument is required, it must not be null.");

		final BoolQueryBuilder pathClauses = boolQuery().should(termQuery("path", path));
		if (includeParent && path.contains(PathUtil.SEPARATOR)) {
			// Pick up the parent branch too
			pathClauses.should(termQuery("path", PathUtil.getParentPath(path)));
		}

		return new NativeSearchQueryBuilder().withQuery(
				new BoolQueryBuilder()
						.must(pathClauses)
						.mustNot(existsQuery("end"))
		).build();
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
				Map<String, String> parentMetadata = parent.getMetadata();
				if (parentMetadata != null) {
					Map<String, String> metadata = branch.getMetadata();
					if (metadata != null) {
						for (String key : parentMetadata.keySet()) {
							if (!metadata.containsKey(key)) {
								metadata.put(key, parentMetadata.get(key));
							}
						}
					} else {
						metadata = parentMetadata;
					}
					branch.setMetadataInternal(metadata);
				}
			}
		}
		return branch;
	}

	public Branch findFirstVersionOrThrow(String path) {
		List<Branch> branchVersions = elasticsearchTemplate.queryForList(
				new NativeSearchQueryBuilder()
						.withQuery(termQuery("path", path))
						.withSort(SortBuilders.fieldSort("start"))
						.withPageable(PageRequest.of(0, 1))
						.build(), Branch.class);
		if (branchVersions.isEmpty()) {
			throw new BranchNotFoundException("Branch '" + path + "' does not exist.");
		}
		return branchVersions.iterator().next();
	}

	public Page<Branch> findAllVersions(String path, Pageable pageable) {
		return elasticsearchTemplate.queryForPage(
				new NativeSearchQueryBuilder()
						.withQuery(termQuery("path", path))
						.withSort(SortBuilders.fieldSort("start"))
						.withPageable(pageable)
						.build(), Branch.class);
	}

	public Branch findAtTimepointOrThrow(String path, Date timepoint) {
		final List<Branch> branches = elasticsearchTemplate.queryForList(new NativeSearchQueryBuilder()
				.withQuery(boolQuery()
						.must(termQuery("path", path))
						.must(rangeQuery("base").lte(timepoint.getTime()))
						.must(boolQuery()
								.should(boolQuery().mustNot(existsQuery("end")))
								.should(rangeQuery("end").gt(timepoint.getTime()))))
				.withSort(SortBuilders.fieldSort("start"))
				.withPageable(PageRequest.of(0, 1))
				.build(), Branch.class);
		if (branches.isEmpty()) {
			throw new IllegalStateException("Branch '" + path + "' does not exist at timepoint " + timepoint + " (" + timepoint.getTime() + ").");
		}

		return branches.get(0);
	}

	public List<Branch> findAll() {
		return elasticsearchTemplate.queryForList(new NativeSearchQueryBuilder()
				.withQuery(new BoolQueryBuilder().mustNot(existsQuery("end")))
				.withSort(new FieldSortBuilder("path"))
				.withPageable(PageRequest.of(0, 10000))
				.build(), Branch.class);
	}

	public List<Branch> findChildren(String path) {
		return findChildren(path, false); //All descendants by default
	}
	
	public List<Branch> findChildren(String path, boolean immediateChildren) {
		List<Branch> children = elasticsearchTemplate.queryForList(new NativeSearchQueryBuilder()
				.withQuery(new BoolQueryBuilder().mustNot(existsQuery("end")).must(prefixQuery("path", path + "/")))
				.withSort(new FieldSortBuilder("path"))
				.build(), Branch.class);
		
		if (immediateChildren) {
			Branch parent = findBranchOrThrow(path);
			children = children.stream()
					.filter(child -> parent.isParent(child))
					.collect(Collectors.toList());
		} 
		return children;
	}

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
			if (commitType == Commit.CommitType.PROMOTION) {
				// Lock source branch as well as target
				lockBranch(mergeSourceBranchPath, sourceBranchLockMetadata);
			}
			Branch branch = lockBranch(branchPath, targetBranchLockMetadata);
			Commit commit = new Commit(branch, commitType, this::completeCommit, this::rollbackCommit);
			logger.info("Open commit on {} at {}", branchPath, commit.getTimepoint().getTime());
			return commit;
		}
	}

	public Branch lockBranch(String branchPath, String lockMetadata) {
		Branch branch = findBranchOrThrow(branchPath);
		if (branch.isLocked()) {
			throw new IllegalStateException(String.format("Branch %s is already locked", branch.getPath()));
		}
		branch.setLocked(true);
		Map<String, String> metadata = branch.getMetadata();
		if (metadata == null) {
			metadata = new HashMap<>();
		}
		metadata.put(LOCK_METADATA_KEY, lockMetadata);
		branch.setMetadata(metadata);
		branch = branchRepository.save(branch);
		return branch;
	}

	public Branch updateMetadata(String path, Map<String, String> metadata) {
		Branch branch = findBranchOrThrow(path);
		branch.setMetadata(metadata);
		return branchRepository.save(branch);
	}

	public Commit openRebaseCommit(String path) {
		return openRebaseCommit(path, null);
	}

	public Commit openRebaseCommit(String path, String lockMetadata) {
		return doOpenRebaseCommit(path, lockMetadata, null);
	}

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
			branch.setBase(parentBranch.getHead());
			commit.setRebasePreviousBase(branch.getBase());
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
		} catch (IllegalStateException e) {
			logger.error("Commit commitListener threw IllegalStateException, rolling back commit {} on branch {}",
					commit.getTimepoint().getTime(), commit.getBranch().getPath(), e);

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
		newBranchTimespan.addVersionsReplaced(oldBranchTimespan.getVersionsReplaced());
		newBranchTimespan.addVersionsReplaced(commit.getEntityVersionsReplaced());
		newBranchTimespan.setCreation(oldBranchTimespan.getCreation());
		newBranchTimespan.setLastPromotion(oldBranchTimespan.getLastPromotion());

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
				newBranchTimespan.addVersionsReplaced(oldSourceBranch.getVersionsReplaced());
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
		branchRepository.saveAll(newBranchVersionsToSave);
		logger.info("Completed commit on {} at {}", commit.getBranch().getPath(), commit.getTimepoint().getTime());
	}

	private void rollbackCommit(Commit commit) {
		logger.info("Rolling back commit on {} started at {}", commit.getBranch().getPath(), commit.getTimepoint().getTime());
		// On all indexes touched: delete documents with the path and timepoint of the commit
		// then remove the write lock from the branch.
		DeleteQuery deleteQuery = new DeleteQuery();
		deleteQuery.setQuery(new BoolQueryBuilder()
				.must(termQuery("path", commit.getBranch().getPath()))
				.must(termQuery("start", commit.getTimepoint().getTime()))
		);
		for (Class domainEntityClass : commit.getDomainEntityClasses()) {
			elasticsearchTemplate.delete(deleteQuery, domainEntityClass);
		}

		Branch branch = commit.getBranch();
		if (commit.isRebase()) {
			resetBranchBase(commit);
		}
		clearLock(branch);

		if (commit.getCommitType() == Commit.CommitType.PROMOTION) {
			unlock(commit.getSourceBranchPath());
		}

		branchRepository.save(branch);
	}

	private void resetBranchBase(Commit commit) {
		commit.getBranch().setBase(commit.getRebasePreviousBase());
	}

	public void unlock(String path) {
		final List<Branch> branches = elasticsearchTemplate.queryForList(new NativeSearchQueryBuilder()
				.withQuery(
					new BoolQueryBuilder()
							.must(termQuery("path", path))
							.mustNot(existsQuery("end"))
					)
				.withPageable(PageRequest.of(0, 1))
				.build(), Branch.class);

		if (!branches.isEmpty()) {
			final Branch branch = branches.get(0);
			clearLock(branch);
			branchRepository.save(branch);
		} else {
			throw new IllegalArgumentException("Branch not found " + path);
		}
	}

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
		Map<String, String> metadata = branchVersion.getMetadata();
		if (metadata != null) {
			metadata.remove(LOCK_METADATA_KEY);
		}
	}

	/**
	 * Get unmodifiable list of commit listeners. To add a listener use the addCommitListener method.
	 * @return unmodifiable list of commit listeners.
	 */
	public List<CommitListener> getCommitListeners() {
		return Collections.unmodifiableList(commitListeners);
	}
}

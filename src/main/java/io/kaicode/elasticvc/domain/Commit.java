package io.kaicode.elasticvc.domain;

import io.kaicode.elasticvc.api.MapUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class Commit implements AutoCloseable {

	private final Branch branch;

	private final Date timepoint;

	private Date rebasePreviousBase;

	/**
	 * Map of classes and internal ids of the entities visible on ancestor branches which have been replaced
	 * or deleted on this branch during this commit.
	 */
	private final Map<String, Set<String>> entityVersionsReplaced;

	private Map<String, Set<String>> versionsReplacedForPromotion;

	/**
	 * Transient set of entity ids of the entities which have been deleted during this commit,
	 * regardless of if they are visible on the parent branch.
	 * This set is just to help with processing deletions during a commit.
	 */
	private final Set<String> entitiesDeleted;
	private final Set<Class<?>> domainEntityClasses;

	private final CommitType commitType;
	private String sourceBranchPath;
	private final Consumer<Commit> onSuccess;
	private final Consumer<Commit> onFailure;
	private boolean successful;

	public Commit(Branch branch, CommitType commitType, Consumer<Commit> onSuccess, Consumer<Commit> onFailure) {
		this.branch = branch;
		this.timepoint = new Date();
		entityVersionsReplaced = new ConcurrentHashMap<>();
		entitiesDeleted = Collections.synchronizedSet(new HashSet<>());
		domainEntityClasses = Collections.synchronizedSet(new HashSet<>());
		this.commitType = commitType;
		this.onSuccess = onSuccess;
		this.onFailure = onFailure;
	}

	public <C extends DomainEntity<?>> void addDomainEntityClass(Class<C> domainEntityClass) {
		this.domainEntityClasses.add(domainEntityClass);
	}

	public void markSuccessful() {
		successful = true;
	}

	@Override
	public void close() {
		if (successful) {
			onSuccess.accept(this);
		} else {
			onFailure.accept(this);
		}
	}

	public Branch getBranch() {
		return branch;
	}

	public Date getTimepoint() {
		return timepoint;
	}

	public CommitType getCommitType() {
		return commitType;
	}

	public void addVersionsReplaced(Set<String> internalIds, Class<? extends DomainEntity<?>> entityClass) {
		entityVersionsReplaced.computeIfAbsent(entityClass.getSimpleName(), (c) -> new HashSet<>()).addAll(internalIds);
	}

	public void addVersionsDeleted(Set<String> entityIds) {
		entitiesDeleted.addAll(entityIds);
	}

	public Map<String, Set<String>> getEntityVersionsReplaced() {
		return entityVersionsReplaced;
	}

	public Map<String, Set<String>> getEntityVersionsReplacedIncludingFromBranch() {
		Map<String, Set<String>> versions = MapUtil.addAll(entityVersionsReplaced, new HashMap<>());
		return MapUtil.addAll(branch.getVersionsReplaced(), versions);
	}

	public Map<String, Set<String>> getVersionsReplacedForPromotion() {
		return versionsReplacedForPromotion;
	}

	public void setVersionsReplacedForPromotion(Map<String, Set<String>> versionsReplacedForPromotion) {
		this.versionsReplacedForPromotion = new ConcurrentHashMap<>(versionsReplacedForPromotion);
	}

	public Set<String> getEntitiesDeleted() {
		return entitiesDeleted;
	}

	public void setSourceBranchPath(String sourceBranchPath) {
		this.sourceBranchPath = sourceBranchPath;
	}

	public String getSourceBranchPath() {
		return sourceBranchPath;
	}

	public boolean isRebase() {
		return CommitType.REBASE == commitType;
	}

	public void setRebasePreviousBase(Date rebasePreviousBase) {
		this.rebasePreviousBase = rebasePreviousBase;
	}

	public Date getRebasePreviousBase() {
		return rebasePreviousBase;
	}

	public Set<Class<?>> getDomainEntityClasses() {
		return domainEntityClasses;
	}

	@Override
	public String toString() {
		return "Commit{" +
				"branch=" + branch +
				", timepoint=" + timepoint +
				'}';
	}

	public enum CommitType {
		CONTENT, REBASE, PROMOTION
	}
}

package io.kaicode.elasticvc.domain;

import io.kaicode.elasticvc.api.MapUtil;

import java.util.*;
import java.util.function.Consumer;

public class Commit implements AutoCloseable {

	private final Branch branch;

	private final Date timepoint;
	private Date rebasePreviousBase;

	private final Map<String, Set<String>> entityVersionsReplaced;
	private final Set<String> entityVersionsDeleted;
	private final Set<Class> domainEntityClasses;

	private final CommitType commitType;
	private String sourceBranchPath;
	private final Consumer<Commit> onSuccess;
	private final Consumer<Commit> onFailure;
	private boolean successful;

	public Commit(Branch branch, CommitType commitType, Consumer<Commit> onSuccess, Consumer<Commit> onFailure) {
		this.branch = branch;
		this.timepoint = new Date();
		entityVersionsReplaced = new HashMap<>();
		entityVersionsDeleted = Collections.synchronizedSet(new HashSet<>());
		domainEntityClasses = Collections.synchronizedSet(new HashSet<>());
		this.commitType = commitType;
		this.onSuccess = onSuccess;
		this.onFailure = onFailure;
	}

	public <C extends DomainEntity> void addDomainEntityClass(Class<C> domainEntityClass) {
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

	public void addVersionsReplaced(Set<String> internalIds, Class<? extends DomainEntity> entityClass) {
		entityVersionsReplaced.computeIfAbsent(entityClass.getSimpleName(), (c) -> new HashSet<>()).addAll(internalIds);
	}

	public void addVersionsDeleted(Set<String> internalIds) {
		entityVersionsDeleted.addAll(internalIds);
	}

	public Map<String, Set<String>> getEntityVersionsReplaced() {
		return entityVersionsReplaced;
	}

	public Map<String, Set<String>> getEntityVersionsReplacedIncludingFromBranch() {
		Map<String, Set<String>> versions = MapUtil.addAll(entityVersionsReplaced, new HashMap<>());
		return MapUtil.addAll(branch.getVersionsReplaced(), versions);
	}

	public Set<String> getEntityVersionsDeleted() {
		return entityVersionsDeleted;
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

	public Set<Class> getDomainEntityClasses() {
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
		CONTENT, REBASE, PROMOTION;
	}
}

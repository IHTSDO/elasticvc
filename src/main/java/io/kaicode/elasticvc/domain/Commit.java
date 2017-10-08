package io.kaicode.elasticvc.domain;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class Commit implements AutoCloseable {

	private final Branch branch;

	private final Date timepoint;
	private Date rebasePreviousBase;

	private final Set<String> entityVersionsReplaced;
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
		entityVersionsReplaced = Collections.synchronizedSet(new HashSet<>());
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

	public void addVersionsReplaced(Set<String> internalIds) {
		entityVersionsReplaced.addAll(internalIds);
	}

	public void addVersionsDeleted(Set<String> internalIds) {
		entityVersionsDeleted.addAll(internalIds);
	}

	public void addVersionReplaced(String internalId) {
		entityVersionsReplaced.add(internalId);
	}

	public void addVersionDeleted(String internalId) {
		entityVersionsDeleted.add(internalId);
	}

	public Set<String> getEntityVersionsReplaced() {
		return entityVersionsReplaced;
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

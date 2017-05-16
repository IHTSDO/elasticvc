package io.kaicode.elasticvc.domain;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class Commit {

	private Branch branch;

	private Date timepoint;
	private Date rebasePreviousBase;

	private Set<String> entityVersionsReplaced;
	private Set<String> entityVersionsDeleted;
	private Set<String> versionsDeletedOnParentFromRebase;

	private CommitType commitType;
	private String sourceBranchPath;

	public Commit(Branch branch, CommitType commitType) {
		this.branch = branch;
		this.timepoint = new Date();
		entityVersionsReplaced = new HashSet<>();
		entityVersionsDeleted = new HashSet<>();
		this.commitType = commitType;
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

	@Override
	public String toString() {
		return "Commit{" +
				"branch=" + branch +
				", timepoint=" + timepoint +
				'}';
	}

	public String getFlatBranchPath() {
		return branch.getFlatPath();
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

	public Set<String> getVersionsDeletedOnParentFromRebase() {
		return versionsDeletedOnParentFromRebase;
	}

	public void setVersionsDeletedOnParentFromRebase(Set<String> versionsDeletedOnParentFromRebase) {
		this.versionsDeletedOnParentFromRebase = versionsDeletedOnParentFromRebase;
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

	public enum CommitType {
		CONTENT, REBASE, PROMOTION
	}
}

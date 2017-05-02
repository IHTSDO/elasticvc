package io.kaicode.elasticvc.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldIndex;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Document(type = "branch", indexName = "branch", shards = 8)
public class Branch extends Entity {

	@Field(type = FieldType.Date, index = FieldIndex.not_analyzed)
	private Date base;

	@Field(type = FieldType.Date, index = FieldIndex.not_analyzed)
	private Date head;

	@Field(type = FieldType.Date, index = FieldIndex.not_analyzed)
	private Date lastPromotion;

	@Field(type = FieldType.Boolean, index = FieldIndex.not_analyzed)
	private boolean locked;

	@Field(type = FieldType.Boolean, index = FieldIndex.not_analyzed)
	private boolean containsContent;

	// The internal ids of entities visible on ancestor branches which have been replaced or deleted on this branch
	private Set<String> versionsReplaced;

	private BranchState state;

	public enum BranchState {
		UP_TO_DATE, FORWARD, BEHIND, DIVERGED;
	}

	public Branch() {
		head = new Date();
		versionsReplaced = new HashSet<>();
	}

	public Branch(String path) {
		this();
		setPath(path);
	}

	public void updateState(Date parentBranchHead) {
		final long parentHeadTimestamp = parentBranchHead.getTime();
		if (parentHeadTimestamp <= getBaseTimestamp()) {
			if (containsContent) {
				state = BranchState.FORWARD;
			} else {
				state = BranchState.UP_TO_DATE;
			}
		} else {
			if (containsContent) {
				state = BranchState.DIVERGED;
			} else {
				state = BranchState.BEHIND;
			}
		}
	}

	public boolean isParent(Branch otherBranch) {
		final String childPath = otherBranch.getFatPath();
		final int endIndex = childPath.lastIndexOf("/");
		return endIndex > 0 && getFatPath().equals(childPath.substring(0, endIndex));
	}

	public void addVersionsReplaced(Set<String> internalIds) {
		if (notMAIN()) {
			versionsReplaced.addAll(internalIds);
		}
	}

	private boolean notMAIN() {
		return !"MAIN".equals(getPath());
	}

	public long getBaseTimestamp() {
		return base.getTime();
	}

	public long getHeadTimestamp() {
		return head.getTime();
	}

	@JsonIgnore
	public String getFlatPath() {
		return getPath();
	}

	public void setHead(Date head) {
		this.head = head;
	}

	public Date getBase() {
		return base;
	}

	public void setBase(Date base) {
		this.base = base;
	}

	public Date getHead() {
		return head;
	}

	public Date getLastPromotion() {
		return lastPromotion;
	}

	public void setLastPromotion(Date lastPromotion) {
		this.lastPromotion = lastPromotion;
	}

	public void setLocked(boolean locked) {
		this.locked = locked;
	}

	public boolean isLocked() {
		return locked;
	}

	public boolean isContainsContent() {
		return containsContent;
	}

	public void setContainsContent(boolean containsContent) {
		this.containsContent = containsContent;
	}

	public Set<String> getVersionsReplaced() {
		return versionsReplaced;
	}

	public void setVersionsReplaced(Set<String> versionsReplaced) {
		this.versionsReplaced = versionsReplaced;
	}

	public BranchState getState() {
		return state;
	}

	public Branch setState(BranchState state) {
		this.state = state;
		return this;
	}

	@Override
	public String toString() {
		return "Branch{" +
				"path=" + getPath() +
				", base=" + getMillis(base) +
				", head=" + getMillis(head) +
				", start=" + getMillis(getStart()) +
				", end=" + getMillis(getEnd()) +
				", versionsReplaced=" + versionsReplaced +
				'}';
	}

	private Long getMillis(Date date) {
		return date == null ? null : date.getTime();
	}
}

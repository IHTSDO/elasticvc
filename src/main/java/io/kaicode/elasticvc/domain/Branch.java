package io.kaicode.elasticvc.domain;

import io.kaicode.elasticvc.api.MapUtil;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.*;
import java.util.stream.Collectors;

@Document(indexName = "branch")
public class Branch extends Entity {

	/**
	 * The root branch path.
	 */
	public static final String MAIN = "MAIN";

	@Field(type = FieldType.Long)
	private Date base;

	@Field(type = FieldType.Long)
	private Date head;

	@Field(type = FieldType.Long)
	private Date creation;

	@Field(type = FieldType.Long)
	private Date lastPromotion;

	@Field(type = FieldType.Boolean)
	private boolean locked;

	@Field(type = FieldType.Boolean)
	private boolean containsContent;

	/**
	 * Map of classes and internal ids of entities visible on ancestor branches which have been replaced or deleted on this branch
	 */
	private Map<String, Collection<String>> versionsReplaced;

	@Field(type = FieldType.Object)
	private Map<String, String> metadataInternal;

	@Transient
	private BranchState state;

	public enum BranchState {
		UP_TO_DATE, FORWARD, BEHIND, DIVERGED;

	}

	public Branch() {
		head = new Date();
		versionsReplaced = new HashMap<>();
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
		final String childPath = otherBranch.getPath();
		final int endIndex = childPath.lastIndexOf("/");
		return endIndex > 0 && getPath().equals(childPath.substring(0, endIndex));
	}

	public void addVersionsReplaced(Map<String, Set<String>> versionsReplacedToAdd) {
		for (String key : versionsReplacedToAdd.keySet()) {
			versionsReplaced.computeIfAbsent(key, (k) -> new HashSet<>()).addAll(versionsReplacedToAdd.get(key));
		}
	}

	public void addVersionsReplaced(DomainEntity entity, Set<String> internalIds) {
		if (notMAIN()) {
			versionsReplaced.computeIfAbsent(entity.getClass().getSimpleName(), (clazz) -> new HashSet<>()).addAll(internalIds);
		}
	}

	public Set<String> getVersionsReplaced(Class<? extends DomainEntity> entityClass) {
		if (notMAIN()) {
			return new HashSet<>(versionsReplaced.getOrDefault(entityClass.getSimpleName(), Collections.emptySet()));
		} else {
			return Collections.emptySet();
		}
	}

	private boolean notMAIN() {
		return !MAIN.equals(getPath());
	}

	public long getBaseTimestamp() {
		return base.getTime();
	}

	public long getHeadTimestamp() {
		return head.getTime();
	}

	public long getCreationTimestamp() {
		return creation.getTime();
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

	public Date getCreation() {
		return creation;
	}

	public void setCreation(Date creation) {
		this.creation = creation;
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

	public Map<String, Set<String>> getVersionsReplaced() {
		return MapUtil.convertToSet(versionsReplaced);
	}

	public Map<String, Integer> getVersionsReplacedCounts() {
		Map<String, Integer> counts = new HashMap<>();
		for (String key : versionsReplaced.keySet()) {
			counts.put(key, versionsReplaced.get(key).size());
		}
		return counts;
	}

	public void setVersionsReplaced(Map<String, Set<String>> versionsReplaced) {
		this.versionsReplaced = MapUtil.convertToCollection(versionsReplaced);
	}

	public BranchState getState() {
		return state;
	}

	public Branch setState(BranchState state) {
		this.state = state;
		return this;
	}

	public Map<String, String> getMetadata() {
		return replaceMapKeyCharacters(metadataInternal, "|", ".");
	}

	public void setMetadata(Map<String, String> metadata) {
		this.metadataInternal = replaceMapKeyCharacters(metadata, ".", "|");
	}

	public Map<String, String> getMetadataInternal() {
		return metadataInternal;
	}

	public void setMetadataInternal(Map<String, String> metadataInternal) {
		this.metadataInternal = metadataInternal;
	}

	private Map<String, String> replaceMapKeyCharacters(Map<String, String> metadata, String s1, String s2) {
		if (metadata != null) {
			Set<String> replaceKeys = metadata.keySet().stream().filter(k -> k.contains(s1)).collect(Collectors.toSet());
			for (String key : replaceKeys) {
				metadata.put(key.replace(s1, s2), metadata.get(key));
				metadata.remove(key);
			}
		}
		return metadata;
	}

	@Override
	public String toString() {
		return "Branch{" +
				"path=" + getPath() +
				", base=" + getMillis(base) +
				", head=" + getMillis(head) +
				", start=" + getMillis(getStart()) +
				", end=" + getMillis(getEnd()) +
				'}';
	}

	private Long getMillis(Date date) {
		return date == null ? null : date.getTime();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Branch branch = (Branch) o;
		return Objects.equals(getPath(), branch.getPath()) &&
				Objects.equals(base, branch.base) &&
				Objects.equals(head, branch.head) &&
				Objects.equals(lastPromotion, branch.lastPromotion);
	}

	@Override
	public int hashCode() {
		return Objects.hash(getPath(), base, head, lastPromotion);
	}
}

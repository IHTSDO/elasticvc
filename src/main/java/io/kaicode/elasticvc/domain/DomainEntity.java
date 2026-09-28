package io.kaicode.elasticvc.domain;

import org.springframework.data.annotation.Transient;

public abstract class DomainEntity<C> extends Entity {

	@Transient
	private boolean changed;

	public abstract String getId();

	public abstract boolean isComponentChanged(C existingComponent);

	/**
	 * Used when saving on a child branch to detect a version with the same content as the version visible on an ancestor branch.
	 * When true the ancestor version is kept visible instead of being replaced by an identical copy.
	 * Defaults to false, which always persists the new version.
	 * @param ancestorVersion	The version of this entity visible on the ancestor branches.
	 * @return	true if this version has the same content as the ancestor version.
	 */
	public boolean isSameContentAs(C ancestorVersion) {
		return false;
	}

	public void setChanged(boolean changed) {
		this.changed = changed;
	}

	public void markChanged() {
		setChanged(true);
	}

	public boolean isChanged() {
		return changed;
	}
}

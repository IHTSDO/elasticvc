package io.kaicode.elasticvc.domain;

import org.springframework.data.annotation.Transient;

public abstract class DomainEntity<C> extends Entity {

	@Transient
	private boolean changed;

	public abstract String getId();

	public abstract boolean isComponentChanged(C existingComponent);

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

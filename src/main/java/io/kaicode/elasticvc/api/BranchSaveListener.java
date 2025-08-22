package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Branch;

public interface BranchSaveListener {

	/**
	 * @param branch the branch to be saved
	 */
	void postSaveCompletion(Branch branch) throws IllegalStateException;
}

package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Commit;

public interface CommitListener {

	/**
	 * If IllegalStateException is thrown the commit will be rolled back
	 * @param commit the commit to be completed
	 * @throws IllegalStateException if the commit should be rolled back
	 */
	void preCommitCompletion(Commit commit) throws IllegalStateException;
}

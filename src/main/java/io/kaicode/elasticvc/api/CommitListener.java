package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Commit;

public interface CommitListener {

	/**
	 * If IllegalStateException is thrown the commit will be rolled back
	 * @param commit
	 * @throws IllegalStateException
	 */
	void preCommitCompletion(Commit commit) throws IllegalStateException;
}

package io.kaicode.elasticvc.api;

import io.kaicode.elasticvc.domain.Commit;

public interface CommitListener {
	void preCommitCompletion(Commit commit);
}

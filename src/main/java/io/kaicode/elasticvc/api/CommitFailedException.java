package io.kaicode.elasticvc.api;

public class CommitFailedException extends ElasticVCRuntimeException {

	public CommitFailedException(String message, Throwable cause) {
		super(message, cause);
	}
}

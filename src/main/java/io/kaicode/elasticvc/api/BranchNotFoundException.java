package io.kaicode.elasticvc.api;

public class BranchNotFoundException extends ElasticVCRuntimeException {

	public BranchNotFoundException(String message) {
		super(message);
	}
}

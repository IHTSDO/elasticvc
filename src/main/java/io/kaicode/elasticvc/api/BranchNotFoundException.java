package io.kaicode.elasticvc.api;

public class BranchNotFoundException extends RuntimeException {

	public BranchNotFoundException(String message) {
		super(message);
	}
}

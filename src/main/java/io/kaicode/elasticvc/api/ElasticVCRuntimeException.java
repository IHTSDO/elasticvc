package io.kaicode.elasticvc.api;

public class ElasticVCRuntimeException extends RuntimeException {

	public ElasticVCRuntimeException(String message) {
		super(message);
	}

	public ElasticVCRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}
}

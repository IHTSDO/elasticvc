package io.kaicode.elasticvc.repositories.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kaicode.elasticvc.domain.Branch;

import java.util.Map;

public abstract class BranchStoreMixIn {

	@JsonIgnore
	abstract Branch.BranchState getState();

	@JsonIgnore
	abstract Map<String, String> getMetadata();

}

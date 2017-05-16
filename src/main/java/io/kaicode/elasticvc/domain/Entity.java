package io.kaicode.elasticvc.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kaicode.elasticvc.api.PathUtil;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldIndex;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Entity {

	public static final SimpleDateFormat DEBUG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	@Id
	@Field(index = FieldIndex.not_analyzed)
	private String internalId;

	@Field(type = FieldType.String, index = FieldIndex.not_analyzed)
	private String path;

	@Field(type = FieldType.Date, index = FieldIndex.not_analyzed)
	private Date start;

	@Field(type = FieldType.Date, index = FieldIndex.not_analyzed)
	private Date end;

	@JsonIgnore
	private boolean deleted;

	public void markDeleted() {
		deleted = true;
	}

	public Entity clearInternalId() {
		internalId = null;
		return this;
	}

	@JsonIgnore
	public String getFatPath() {
		return PathUtil.fatten(path);
	}

	public String getInternalId() {
		return internalId;
	}

	public void setInternalId(String internalId) {
		this.internalId = internalId;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = PathUtil.flatten(path);
	}

	public Date getStart() {
		return start;
	}

	public String getStartDebugFormat() {
		return start == null ? null : DEBUG_DATE_FORMAT.format(start);
	}

	public void setStart(Date start) {
		this.start = start;
	}

	public Date getEnd() {
		return end;
	}

	public String getEndDebugFormat() {
		return end == null ? null : DEBUG_DATE_FORMAT.format(end);
	}

	public void setEnd(Date end) {
		this.end = end;
	}

	public boolean isDeleted() {
		return deleted;
	}

}

package io.kaicode.elasticvc.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Entity {

	public static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STRING);

	@Id
	@Field(type = FieldType.Keyword)
	private String internalId;

	@Field(type = FieldType.Keyword)
	private String path;

	@Field(type = FieldType.Long)
	private Date start;

	@Field(type = FieldType.Long)
	private Date end;

	@Transient
	private boolean deleted;

	public void markDeleted() {
		deleted = true;
	}

	public Entity clearInternalId() {
		internalId = null;
		return this;
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
		this.path = path;
	}

	public Date getStart() {
		return start;
	}

	@JsonIgnore
	public String getStartDebugFormat() {
		return start == null ? null : DATE_FORMAT.format(start);
	}

	public void setStart(Date start) {
		this.start = start;
	}

	public Date getEnd() {
		return end;
	}

	@JsonIgnore
	public String getEndDebugFormat() {
		return end == null ? null : DATE_FORMAT.format(end);
	}

	public void setEnd(Date end) {
		this.end = end;
	}

	public boolean isDeleted() {
		return deleted;
	}

	@Override
	public String toString() {
		return "Entity{" +
				"internalId='" + internalId + '\'' +
				", path='" + path + '\'' +
				", start=" + start +
				", end=" + end +
				", deleted=" + deleted +
				'}';
	}
}

package io.kaicode.elasticvc.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonRawValue;

import java.util.*;

public class Metadata {

	@JsonIgnore
	private final Map<String, Object> internalMap;

	public Metadata() {
		internalMap = new HashMap<>();
	}

	public Metadata(Map<String, Object> internalMap) {
		if (internalMap == null) {
			internalMap = new HashMap<>();
		}
		this.internalMap = internalMap;
	}

	public boolean containsKey(String key) {
		return internalMap.containsKey(key);
	}

	public Metadata putString(String key, String value) {
		internalMap.put(key, value);
		return this;
	}

	public String getString(String key) {
		return (String) internalMap.get(key);
	}

	public List<String> getList(String key) {
		@SuppressWarnings("unchecked")
		final List<String> list = (List<String>) internalMap.get(key);
		return list;
	}

	public Metadata putMap(String key, Map<String, String> value) {
		internalMap.put(key, value);
		return this;
	}

	public Map<String, String> getMapOrCreate(String key) {
		Map<String, String> map = getMap(key);
		if (map == null) {
			map = new HashMap<>();
			putMap(key, map);
		}
		return map;
	}

	public Map<String, String> getMap(String key) {
		@SuppressWarnings("unchecked")
		final Map<String, String> stringStringMap = (Map<String, String>) internalMap.get(key);
		return stringStringMap;
	}

	public void remove(String key) {
		internalMap.remove(key);
	}

	public void putAll(Map<String, Object> metadata) {
		internalMap.putAll(metadata);
	}

	@JsonRawValue
	public Map<String, Object> getAsMap() {
		return internalMap;
	}

	public int size() {
		return internalMap.size();
	}

	@Override
	public String toString() {
		return "Metadata{" +
				"map=" + internalMap +
				'}';
	}
}

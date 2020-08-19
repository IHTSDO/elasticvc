package io.kaicode.elasticvc.api;

import java.util.*;

public class MapUtil {
	public static Map<String, Set<String>> addAll(Map<String, Set<String>> source, Map<String, Set<String>> destination) {
		for (String key : source.keySet()) {
			Set<String> allValues = new HashSet<>(destination.getOrDefault(key, Collections.emptySet()));
			allValues.addAll(source.get(key));
			destination.put(key, allValues);
		}
		return destination;
	}

	public static Map<String, Set<String>> convertToSet(Map<String, Collection<String>> source) {
		Map<String, Set<String>> result = new HashMap<>();
		for (String key : source.keySet()) {
			result.put(key, new HashSet<>(source.get(key)));
		}
		return result;
	}

	public static Map<String, Collection<String>> convertToCollection(Map<String, Set<String>> source) {
		Map<String, Collection<String>> result = new HashMap<>();
		for (String key : source.keySet()) {
			result.put(key, source.get(key));
		}
		return result;
	}
}

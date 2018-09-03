package io.kaicode.elasticvc.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MapUtil {
	public static Map<String, Set<String>> addAll(Map<String, Set<String>> source, Map<String, Set<String>> destination) {
		for (String key : source.keySet()) {
			Set<String> allValues = new HashSet<>(destination.getOrDefault(key, Collections.emptySet()));
			allValues.addAll(source.get(key));
			destination.put(key, allValues);
		}
		return destination;
	}
}

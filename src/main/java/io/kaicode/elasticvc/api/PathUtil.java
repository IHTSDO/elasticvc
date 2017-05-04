package io.kaicode.elasticvc.api;

public class PathUtil {

	public static final String SEPARATOR = "/";

	public static String getParentPath(String path) {
		path = fatten(path);
		final int indexOf = path.lastIndexOf(SEPARATOR);
		if (indexOf != -1) {
			return path.substring(0, indexOf);
		}
		return null;
	}

	public static String flaten(String path) {
		if (path != null) {
			return path.replace("/", "_");
		} else {
			throw new RuntimeException("Path is null");
		}
	}

	public static String fatten(String path) {
		return path.replace("_", "/");
	}

	public static boolean isRoot(String path) {
		return !fatten(path).contains(SEPARATOR);
	}
}

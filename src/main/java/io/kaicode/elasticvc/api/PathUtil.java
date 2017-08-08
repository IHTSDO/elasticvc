package io.kaicode.elasticvc.api;

public class PathUtil {

	public static final String SEPARATOR = "/";

	public static String getParentPath(String path) {
		final int indexOf = path.lastIndexOf(SEPARATOR);
		if (indexOf != -1) {
			return path.substring(0, indexOf);
		}
		return null;
	}

	public static boolean isRoot(String path) {
		return !path.contains(SEPARATOR);
	}
}

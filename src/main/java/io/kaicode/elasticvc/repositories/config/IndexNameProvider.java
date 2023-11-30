package io.kaicode.elasticvc.repositories.config;

public class IndexNameProvider {

    private final String prefix;

    public IndexNameProvider(String indexPrefix) {
        this.prefix = indexPrefix;
    }

    public String indexName(String indexName) {
        return prefix == null || prefix.isEmpty() ? indexName : prefix + indexName;
    }
}

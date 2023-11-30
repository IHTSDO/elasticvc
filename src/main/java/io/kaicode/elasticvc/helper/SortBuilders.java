package io.kaicode.elasticvc.helper;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;

public class SortBuilders {
    public static SortOptions fieldSort(String fieldName) {
        return fieldSortAsc(fieldName);
    }

    public static SortOptions fieldSortDesc(String fieldName) {
        return SortOptions.of(s -> s.field(f -> f.field(fieldName)));
    }

    public static SortOptions fieldSortAsc(String fieldName) {
        return SortOptions.of(s -> s.field(f -> f.field(fieldName).order(SortOrder.Asc)));
    }
}

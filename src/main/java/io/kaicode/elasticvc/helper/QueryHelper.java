package io.kaicode.elasticvc.helper;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.json.JsonData;

import java.util.Collection;

public class QueryHelper {

    public static Query termsQuery(String field, Collection<?> values) {
        return new TermsQuery.Builder().field(field).terms(tq -> tq.value(values.stream().map(JsonData::of).map(FieldValue::of).toList())).build()._toQuery();
    }

    public static Query existsQuery(String field) {
        return new ExistsQuery.Builder().field(field).build()._toQuery();
    }

    public static Query termQuery(String field, Object value) {
        return new TermQuery.Builder().field(field).value(FieldValue.of(JsonData.of(value))).build()._toQuery();
    }
}

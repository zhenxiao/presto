/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch2;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.elasticsearch2.ElasticsearchErrorCode.ELASTIC_SEARCH_EXCEEDS_MAX_HIT_ERROR;
import static com.facebook.presto.elasticsearch2.ElasticsearchUtils.serializeObject;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordCursor
        implements RecordCursor
{
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final Map<String, Integer> jsonPathToIndex = new HashMap();
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final int maxHits;
    private final Iterator<SearchHit> searchHits;
    private final Duration timeout;
    private final boolean isNested;

    private long totalBytes;
    private List<Object> fields;

    public ElasticsearchRecordCursor(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        this.columnHandles = requireNonNull(columnHandles, "ElasticsearchColumnHandle is null");
        this.maxHits = requireNonNull(config, "ElasticsearchConnectorConfig is null").getMaxHits();
        this.tupleDomain = requireNonNull(split, "ElasticsearchConnectorSplit is null").getTupleDomain();
        this.timeout = config.getRequestTimeout();
        this.isNested = columnHandles.stream().anyMatch(c -> c.getColumnType().getTypeSignature().getBase().equals(ROW) || c.getColumnType().getTypeSignature().getBase().equals(ARRAY) || c.getColumnType().getTypeSignature().getBase().equals(MAP));

        for (int i = 0; i < columnHandles.size(); i++) {
            jsonPathToIndex.put(columnHandles.get(i).getColumnJsonPath(), i);
        }
        ElasticsearchQueryBuilder builder = new ElasticsearchQueryBuilder(columnHandles, config, split, isNested);
        searchHits = sendElasticQuery(builder).iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!searchHits.hasNext()) {
            return false;
        }

        SearchHit hit = searchHits.next();
        fields = new ArrayList(Collections.nCopies(columnHandles.size(), null));

        setFieldIfExists("_id", hit.getId());
        setFieldIfExists("_index", hit.getIndex());

        if (isNested) {
            extractFromSource(hit);
        }
        else {
            extractFromHitField(hit);
        }

        totalBytes += fields.size();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ImmutableSet.of(BOOLEAN));
        return (Boolean) getFieldValue(field);
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ImmutableSet.of(BIGINT, INTEGER));
        return Long.valueOf(String.valueOf(getFieldValue(field)));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ImmutableSet.of(DOUBLE));
        return (Double) getFieldValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, ImmutableSet.of(VARCHAR));

        Object value = getFieldValue(field);
        ElasticsearchColumnHandle column = columnHandles.get(field);
        if (value instanceof Collection) {
            return utf8Slice(new JSONArray((List<Map<String, Object>>) value).toString());
        }
        return utf8Slice(String.valueOf(value));
    }

    @Override
    public Object getObject(int field)
    {
        return serializeObject(columnHandles.get(field).getColumnType(), null, getFieldValue(field));
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return getFieldValue(field) == null;
    }

    private void checkFieldType(int field, Set<Type> expectedTypes)
    {
        Type actual = getType(field);
        checkArgument(expectedTypes.contains(actual), "Field %s expected type inconsistent with %s", field, actual);
        StringBuilder builder = new StringBuilder();
        for (Type type : expectedTypes) {
            builder.append(", ");
            builder.append(type.getDisplayName());
        }
    }

    @Override
    public void close()
    {
    }

    private List<SearchHit> sendElasticQuery(ElasticsearchQueryBuilder queryBuilder)
    {
        List<SearchHit> result = new ArrayList<>();
        SearchResponse response = queryBuilder.buildScrollSearchRequest().execute().actionGet(timeout.toMillis());

        if (response.getHits().getTotalHits() > maxHits) {
            throw new PrestoException(ELASTIC_SEARCH_EXCEEDS_MAX_HIT_ERROR, "The number of hits for the query: " + response.getHits().getTotalHits() + " exceeds the configured max hits: " + maxHits);
        }

        while (true) {
            Collections.addAll(result, response.getHits().getHits());
            response = queryBuilder.prepareSearchScroll(response.getScrollId()).execute().actionGet(timeout.toMillis());
            if (response.getHits().getHits().length == 0) {
                break;
            }
        }
        return result;
    }

    private void setFieldIfExists(String key, Object value)
    {
        if (jsonPathToIndex.containsKey(key)) {
            fields.set(jsonPathToIndex.get(key), value);
        }
    }

    private Object getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
    }

    private void extractFromSource(SearchHit hit)
    {
        Map<String, Object> map = hit.getSource();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String jsonPath = entry.getKey();
            Object entryValue = entry.getValue();

            setFieldIfExists(jsonPath, entryValue);
        }
    }

    private void extractFromHitField(SearchHit hit)
    {
        Map<String, SearchHitField> map = hit.getFields();
        for (Map.Entry<String, SearchHitField> entry : map.entrySet()) {
            String jsonPath = entry.getKey().toString();
            Object entryValue = entry.getValue().getValue();

            setFieldIfExists(jsonPath, entryValue);
        }
    }
}

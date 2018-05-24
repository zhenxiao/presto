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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.RegexpQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTIC_SEARCH_CLIENT_ERROR;
import static com.facebook.presto.spi.predicate.Marker.Bound.ABOVE;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ElasticsearchQueryBuilder
{
    private static final Logger log = Logger.get(ElasticsearchQueryBuilder.class);

    private final Duration scrollTime;
    private final int scrollSize;
    private final Client client;
    private final int shard;
    private final TupleDomain<ColumnHandle> tupleDomain;
    private final List<ElasticsearchColumnHandle> columns;
    private final String index;
    private final String type;
    private final Optional<Map<String, String>> jsonPaths;
    private final Optional<Long> limit;
    private final Optional<TupleDomain<List<String>>> nestedTupleDomain;

    public ElasticsearchQueryBuilder(List<ElasticsearchColumnHandle> columnHandles, ElasticsearchConnectorConfig config, ElasticsearchSplit split)
    {
        columns = requireNonNull(columnHandles, "ElasticsearchColumnHandle is null");
        requireNonNull(config, "ElasticsearchConnectorConfig is null");
        requireNonNull(split, "ElasticsearchSplit is null");
        ElasticsearchTableDescription table = split.getTable();
        tupleDomain = split.getTupleDomain();
        index = split.getIndex();
        type = table.getType();
        shard = split.getShard();
        try {
            Settings settings = Settings.builder().put("client.transport.ignore_cluster_name", true).build();
            client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(split.getSearchNode()), split.getPort()));
        }
        catch (IOException e) {
            throw new PrestoException(ELASTIC_SEARCH_CLIENT_ERROR, "Error connecting to Elasticsearch SearchNode: " + split.getSearchNode() + " with port: " + split.getPort(), e);
        }
        scrollTime = config.getScrollTime();
        scrollSize = config.getScrollSize();
        jsonPaths = split.getJsonPaths();
        limit = split.getLimit();
        nestedTupleDomain = split.getNestedTupleDomain();
    }

    public SearchRequestBuilder buildScrollSearchRequest()
    {
        StringBuilder shardBuilder = new StringBuilder("_shards:");
        shardBuilder.append(shard);
        String indices = index != null && !index.isEmpty() ? index : "_all";
        int size = limit.isPresent() ? limit.get().intValue() : scrollSize;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indices)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setScroll(new TimeValue(scrollTime.toMillis()))
                .setQuery(buildSearchQuery())
                .setPreference(shardBuilder.toString())
                .setSize(size);
        log.debug("ElasticSearch Request: " + searchRequestBuilder.toString());
        return searchRequestBuilder;
    }

    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId)
    {
        return client.prepareSearchScroll(scrollId).setScroll(new TimeValue(scrollTime.toMillis()));
    }

    private QueryBuilder buildSearchQuery()
    {
        BoolQueryBuilder elasticQueryBuilder = new BoolQueryBuilder();
        for (ElasticsearchColumnHandle column : columns) {
            Type type = column.getColumnType();
            tupleDomain.getDomains().ifPresent((domains) -> {
                Domain domain = domains.get(column);
                if (domain != null) {
                    elasticQueryBuilder.must(buildPredicate(column.getColumnJsonPath(), domain, type));
                }
            });
            if (jsonPaths.isPresent() && jsonPaths.get().containsKey(column.getColumnJsonPath().toLowerCase())) {
                String columnName = column.getColumnJsonPath().toLowerCase();
                elasticQueryBuilder.must(buildRegexpQuery(columnName, jsonPaths.get().get(columnName)));
            }
        }
        if (nestedTupleDomain.isPresent()) {
            TupleDomain<List<String>> nestedDomain = nestedTupleDomain.get();
            nestedDomain.getDomains().ifPresent((domains) -> {
                for (Map.Entry<List<String>, Domain> entry : domains.entrySet()) {
                    StringBuilder stringBuilder = new StringBuilder();
                    for (int i = 0; i < entry.getKey().size(); i++) {
                        if (i == 0) {
                            stringBuilder.append(entry.getKey().get(i));
                        }
                        else {
                            stringBuilder.append(".").append(entry.getKey().get(i));
                        }
                    }
                    String columnName = stringBuilder.toString();
                    Domain domain = entry.getValue();
                    if (domain != null) {
                        elasticQueryBuilder.must(buildPredicate(columnName, domain, VARCHAR));
                    }
                }
            });
        }
        return elasticQueryBuilder.hasClauses() ? elasticQueryBuilder : new MatchAllQueryBuilder();
    }

    private QueryBuilder buildPredicate(String columnName, Domain domain, Type type)
    {
        checkArgument(domain.getType().isOrderable(), "Domain type must be orderable");
        BoolQueryBuilder elasticQueryBuilder = new BoolQueryBuilder();

        if (domain.getValues().isNone() && domain.isNullAllowed()) {
            elasticQueryBuilder.mustNot(new ExistsQueryBuilder(columnName));
            return elasticQueryBuilder;
        }

        if (domain.getValues().isAll()) {
            elasticQueryBuilder.must(new ExistsQueryBuilder(columnName));
            return elasticQueryBuilder;
        }

        return buildTermQuery(elasticQueryBuilder, columnName, domain, type);
    }

    private QueryBuilder buildRegexpQuery(String columnName, String jsonPath)
    {
        checkArgument(jsonPath.startsWith("^$.") || jsonPath.startsWith("$."), "Invalid Json Path: " + jsonPath);
        BoolQueryBuilder builder = new BoolQueryBuilder();
        if (jsonPath.startsWith("^$.")) {
            builder.mustNot(new RegexpQueryBuilder(columnName, ".*" + jsonPath.substring(3) + ".*"));
        }
        else {
            builder.must(new RegexpQueryBuilder(columnName, ".*" + jsonPath.substring(2) + ".*"));
        }
        return builder;
    }

    private QueryBuilder buildMatchQuery(BoolQueryBuilder builder, String columnName, Domain domain, Type type)
    {
        List<Object> includeValues = new ArrayList<>();
        List<Object> excludeValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll());
            if (range.isSingleValue()) {
                includeValues.add(range.getLow().getValue());
            }

            if (!range.getLow().isLowerUnbounded() && range.getLow().getBound() == ABOVE) {
                Object value = range.getLow().getValue();
                if (!excludeValues.contains(value)) {
                    excludeValues.add(value);
                }
            }

            if (!range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == BELOW) {
                Object value = range.getHigh().getValue();
                if (!excludeValues.contains(value)) {
                    excludeValues.add(value);
                }
            }
        }

        for (Object value : excludeValues) {
            builder.mustNot(new MatchQueryBuilder(columnName, getValue(type, value)));
        }

        if (includeValues.size() == 1) {
            builder.must(new MatchQueryBuilder(columnName, getValue(type, getOnlyElement(includeValues))));
        }
        else {
            for (Object value : includeValues) {
                builder.should(new MatchQueryBuilder(columnName, getValue(type, value)));
            }
        }
        return builder;
    }

    private QueryBuilder buildTermQuery(BoolQueryBuilder builder, String columnName, Domain domain, Type type)
    {
        List<Object> includeValues = new ArrayList<>();
        List<Object> excludeValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll());
            if (range.isSingleValue()) {
                includeValues.add(range.getLow().getValue());
            }
            else if (type.equals(VARCHAR)) {
                if (!range.getLow().isLowerUnbounded() && range.getLow().getBound() == ABOVE) {
                    Object value = range.getLow().getValue();
                    if (!excludeValues.contains(value)) {
                        excludeValues.add(value);
                    }
                }
            }
            else {
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            builder.must(new RangeQueryBuilder(columnName).gt(getValue(type, range.getLow().getValue())));
                            break;
                        case EXACTLY:
                            builder.must(new RangeQueryBuilder(columnName).gte(getValue(type, range.getLow().getValue())));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case EXACTLY:
                            builder.must(new RangeQueryBuilder(columnName).lte(getValue(type, range.getHigh().getValue())));
                            break;
                        case BELOW:
                            builder.must(new RangeQueryBuilder(columnName).lt(getValue(type, range.getHigh().getValue())));
                            break;
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
            }
        }

        for (Object value : excludeValues) {
            builder.mustNot(new TermQueryBuilder(columnName, getValue(type, value)));
        }

        if (includeValues.size() == 1) {
            builder.must(new TermQueryBuilder(columnName, getValue(type, getOnlyElement(includeValues))));
        }
        return builder;
    }

    public static Object getValue(Type type, Object value)
    {
        if (type.equals(BIGINT)) {
            return value;
        }
        else if (type.equals(INTEGER)) {
            return ((Number) value).intValue();
        }
        else if (type.equals(DOUBLE)) {
            return value;
        }
        else if (type.equals(VARCHAR)) {
            return ((Slice) value).toStringUtf8();
        }
        else if (type.equals(BOOLEAN)) {
            return value;
        }
        throw new UnsupportedOperationException("Query Builder can't handle type: " + type);
    }
}

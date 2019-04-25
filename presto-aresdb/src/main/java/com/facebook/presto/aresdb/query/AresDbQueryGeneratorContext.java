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

package com.facebook.presto.aresdb.query;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.aresdb.AresDbColumnHandle;
import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.spi.pipeline.PushDownBetweenExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<String, Selection> selections;
    private final List<String> groupByColumns;
    private final String from;
    private final Optional<String> timeColumn;
    private final Optional<PushDownExpression> timeFilter;
    private final String filter;
    private final Long limit;
    private final boolean aggregationApplied;

    AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, Optional<String> timeColumn)
    {
        this(selections, from, timeColumn, Optional.empty(), null, false, ImmutableList.of(), null);
    }

    private AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, Optional<String> timeColumn, Optional<PushDownExpression> timeFilter,
            String filter, boolean aggregationApplied, List<String> groupByColumns, Long limit)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.from = requireNonNull(from, "source can't be null");
        this.aggregationApplied = aggregationApplied;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.filter = filter;
        this.limit = limit;
        this.timeColumn = timeColumn;
        this.timeFilter = timeFilter;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withFilter(String filter, Optional<PushDownExpression> timeFilter)
    {
        checkArgument(!hasFilter(), "There already exists a filter. AresDb doesn't support filters at multiple levels");
        checkArgument(!hasAggregation(), "AresDB doesn't support filtering the results of aggregation");
        checkArgument(!hasLimit(), "AresDB doesn't support filtering on top of the limit");
        return new AresDbQueryGeneratorContext(selections, from, timeColumn, timeFilter, filter, aggregationApplied, groupByColumns, limit);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withAggregation(LinkedHashMap<String, Selection> newSelections, List<String> groupByColumns)
    {
        // there is only one aggregation supported.
        checkArgument(!hasAggregation(), "AresDB doesn't support aggregation on top of the aggregated data");
        checkArgument(!hasLimit(), "AresDB doesn't support aggregation on top of the limit");
        return new AresDbQueryGeneratorContext(newSelections, from, timeColumn, timeFilter, filter, true, groupByColumns, limit);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withProject(LinkedHashMap<String, Selection> newSelections)
    {
        checkArgument(!hasAggregation(), "AresDB doesn't support new selections on top of the aggregated data");
        return new AresDbQueryGeneratorContext(newSelections, from, timeColumn, timeFilter, filter, false, ImmutableList.of(), limit);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withLimit(long limit)
    {
        return new AresDbQueryGeneratorContext(selections, from, timeColumn, timeFilter, filter, aggregationApplied, groupByColumns, limit);
    }

    private boolean hasFilter()
    {
        return filter != null || timeFilter.isPresent();
    }

    private boolean hasAggregation()
    {
        return aggregationApplied;
    }

    private boolean hasLimit()
    {
        return limit != null;
    }

    public LinkedHashMap<String, Selection> getSelections()
    {
        return selections;
    }

    public Optional<String> getTimeColumn()
    {
        return timeColumn;
    }

    /**
     * Convert the current context to a AresDB request (AQL)
     */
    public AugmentedAQL toAresDbRequest(Optional<List<AresDbColumnHandle>> columnHandles)
    {
        List<Selection> measures;
        List<Selection> dimensions;
        if (groupByColumns.isEmpty() && !hasAggregation()) {
            // simple selections
            measures = ImmutableList.of(Selection.of("1", Origin.LITERAL, BIGINT));
            dimensions = selections.values().stream().collect(Collectors.toList());
        }
        else {
            measures = selections.entrySet().stream()
                    .filter(c -> !groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());

            dimensions = selections.entrySet().stream()
                    .filter(c -> groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());
        }

        JSONObject request = new JSONObject();
        JSONArray measuresJson = new JSONArray();
        JSONArray dimensionsJson = new JSONArray();

        for (Selection measure : measures) {
            JSONObject measureJson = new JSONObject();
            measureJson.put("sqlExpression", measure.getDefinition());
            measuresJson.add(measureJson);
        }

        for (Selection dimension : dimensions) {
            JSONObject dimensionJson = new JSONObject();
            dimensionJson.put("sqlExpression", dimension.getDefinition());
            if (dimension.timeTokenizer.isPresent()) {
                dimensionJson.put("timeBucketizer", dimension.timeTokenizer.get());
            }
            dimensionsJson.add(dimensionJson);
        }

        request.put("table", from);
        request.put("measures", measuresJson);
        request.put("dimensions", dimensionsJson);
        if (timeFilter.isPresent()) {
            addTimeFilter(request, timeColumn.get(), timeFilter.get());
        }

        if (filter != null) {
            JSONArray filterJson = new JSONArray();
            filterJson.add(filter);
            request.put("rowFilters", filterJson);
        }

        if (limit != null) {
            request.put("limit", limit);
        }

        String aql = request.toJSONString();

        Optional<List<AresDbOutputInfo>> expectedIndicesInOutput = getIndicesMappingFromAresDbSchemaToPrestoSchema(columnHandles, aql);

        return new AugmentedAQL(aql, expectedIndicesInOutput);
    }

    private Optional<List<AresDbOutputInfo>> getIndicesMappingFromAresDbSchemaToPrestoSchema(Optional<List<AresDbColumnHandle>> columnHandles, String aql)
    {
        LinkedHashMap<String, Selection> expressionsInOrder = new LinkedHashMap<>();
        if (!groupByColumns.isEmpty()) {
            // Sanity check
            for (String groupByColumn : groupByColumns) {
                if (!selections.containsKey(groupByColumn)) {
                    throw new IllegalStateException(format("Group By column (%s) definition not found in input selections: ",
                            groupByColumn, Joiner.on(",").withKeyValueSeparator(":").join(selections)));
                }
            }

            // first add the time bucketizer group by columns
            for (String groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }

            // next add the non-time bucketizer group by columns
            for (String groupByColumn : groupByColumns) {
                Selection groupByColumnDefinition = selections.get(groupByColumn);
                if (!groupByColumnDefinition.getTimeTokenizer().isPresent()) {
                    expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
                }
            }
            // Group by columns come first and have already been added above
            // so we are adding the metrics below
            expressionsInOrder.putAll(selections);
        }
        else {
            expressionsInOrder.putAll(selections);
        }

        return columnHandles.map(handles -> {
            checkState(handles.size() == expressionsInOrder.size(), "Expected returned expressions %s to match column handles %s",
                    Joiner.on(",").withKeyValueSeparator(":").join(expressionsInOrder), Joiner.on(",").join(handles));
            Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < handles.size(); ++i) {
                String columnName = handles.get(i).getColumnName();
                Integer prev = nameToIndex.put(columnName.toLowerCase(ENGLISH), i);
                if (prev != null) {
                    throw new IllegalStateException(format("Expected AresDB column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
                }
            }
            ImmutableList.Builder<AresDbOutputInfo> outputInfos = ImmutableList.builder();
            for (Map.Entry<String, Selection> expression : expressionsInOrder.entrySet()) {
                Integer index = nameToIndex.get(expression.getKey());
                if (index == null) {
                    throw new IllegalStateException(format("Expected to find a AresDB column handle for the expression %s, but we have %s",
                            expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
                }
                outputInfos.add(new AresDbOutputInfo(index, expression.getValue().getTimeTokenizer()));
            }
            return outputInfos.build();
        });
    }

    private void addTimeFilter(JSONObject request, String timeColumn, PushDownExpression timeFilter)
    {
        if (timeFilter instanceof PushDownBetweenExpression) {
            PushDownBetweenExpression betweenExpression = (PushDownBetweenExpression) timeFilter;
            JSONObject timeFilterJson = new JSONObject();
            timeFilterJson.put("column", timeColumn);
            timeFilterJson.put("from", betweenExpression.getLeft().toString());
            timeFilterJson.put("to", betweenExpression.getRight().toString());
            request.put("timeFilter", timeFilterJson);

            return;
        }

        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Unsupported time filter: " + timeFilter);
    }

    public enum Origin
    {
        TABLE,
        LITERAL,
        DERIVED,
    }

    public static class Selection
    {
        private String definition;
        private Origin origin;
        private Type outputType;
        private Optional<String> timeTokenizer;

        private Selection(String definition, Origin origin, Type outputType, Optional<String> timeTokenizer)
        {
            this.definition = definition;
            this.origin = origin;
            this.outputType = outputType;
            this.timeTokenizer = timeTokenizer;
        }

        public static Selection of(String definition, Origin origin, Type outputType)
        {
            return new Selection(definition, origin, outputType, Optional.empty());
        }

        public static Selection of(String definition, Origin origin, Type outputType, Optional<String> timeTokenizer)
        {
            return new Selection(definition, origin, outputType, timeTokenizer);
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        public Type getOutputType()
        {
            return outputType;
        }

        public Optional<String> getTimeTokenizer()
        {
            return timeTokenizer;
        }
    }

    public static class AugmentedAQL
    {
        final String aql;
        final Optional<List<AresDbOutputInfo>> outputInfo;

        public AugmentedAQL(String aql, Optional<List<AresDbOutputInfo>> outputInfo)
        {
            this.aql = aql;
            this.outputInfo = outputInfo;
        }

        public String getAql()
        {
            return aql;
        }

        public Optional<List<AresDbOutputInfo>> getOutputInfo()
        {
            return outputInfo;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("aql", aql)
                    .add("outputInfo", outputInfo)
                    .toString();
        }
    }

    public static class AresDbOutputInfo
    {
        public final int index;
        public final Optional<String> timeBucketizer;

        public AresDbOutputInfo(int index, Optional<String> timeBucketizer)
        {
            this.index = index;
            this.timeBucketizer = timeBucketizer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof AresDbOutputInfo)) {
                return false;
            }

            AresDbOutputInfo that = (AresDbOutputInfo) o;
            return index == that.index && Objects.equals(timeBucketizer, that.timeBucketizer);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(index, timeBucketizer);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .add("timeBucketizer", timeBucketizer)
                    .toString();
        }
    }
}

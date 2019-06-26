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
import com.facebook.presto.aresdb.AresDbConfig;
import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.pipeline.JoinPipelineNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<String, Selection> selections;
    private final List<String> groupByColumns;
    private final AresDbTableHandle tableHandle;
    private final Optional<Domain> timeFilter;
    private final List<String> filters;
    private final Long limit;
    private final boolean aggregationApplied;
    private final List<JoinInfo> joins;

    AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, AresDbTableHandle tableHandle)
    {
        this(selections, tableHandle, Optional.empty(), ImmutableList.of(), false, ImmutableList.of(), null, ImmutableList.of());
    }

    private AresDbQueryGeneratorContext(LinkedHashMap<String, Selection> selections, AresDbTableHandle tableHandle, Optional<Domain> timeFilter,
            List<String> filters, boolean aggregationApplied, List<String> groupByColumns, Long limit, List<JoinInfo> joins)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.tableHandle = requireNonNull(tableHandle, "source can't be null");
        this.aggregationApplied = aggregationApplied;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.filters = filters;
        this.limit = limit;
        this.timeFilter = timeFilter;
        this.joins = joins;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withFilters(List<String> extraFilters, Optional<Domain> extraTimeFilter)
    {
        checkArgument(!hasAggregation(), "AresDB doesn't support filtering the results of aggregation");
        checkArgument(!hasLimit(), "AresDB doesn't support filtering on top of the limit");
        checkArgument(!extraTimeFilter.isPresent() || !this.timeFilter.isPresent(), "Cannot put a time filter on top of a time filter");
        List<String> newFilters = ImmutableList.<String>builder().addAll(this.filters).addAll(extraFilters).build();
        Optional<Domain> newTimeFilter = this.timeFilter.isPresent() ? this.timeFilter : extraTimeFilter;
        return new AresDbQueryGeneratorContext(selections, tableHandle, newTimeFilter, newFilters, aggregationApplied, groupByColumns, limit, joins);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withAggregation(LinkedHashMap<String, Selection> newSelections, List<String> groupByColumns)
    {
        // there is only one aggregation supported.
        checkArgument(!hasAggregation(), "AresDB doesn't support aggregation on top of the aggregated data");
        checkArgument(!hasLimit(), "AresDB doesn't support aggregation on top of the limit");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, timeFilter, filters, true, groupByColumns, limit, joins);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withProject(LinkedHashMap<String, Selection> newSelections)
    {
        checkArgument(!hasAggregation(), "AresDB doesn't support new selections on top of the aggregated data");
        return new AresDbQueryGeneratorContext(newSelections, tableHandle, timeFilter, filters, false, ImmutableList.of(), limit, joins);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    AresDbQueryGeneratorContext withLimit(long limit)
    {
        return new AresDbQueryGeneratorContext(selections, tableHandle, timeFilter, filters, aggregationApplied, groupByColumns, limit, joins);
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
        return tableHandle.getTimeColumnName();
    }

    private static ISOChronology getChronology(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return ISOChronology.getInstanceUTC();
        }
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
        return ISOChronology.getInstance(dateTimeZone);
    }

    // Stolen from com.facebook.presto.operator.scalar.DateTimeFunctions.localTime
    private static long getSessionStartTimeInMs(ConnectorSession session)
    {
        if (session.isLegacyTimestamp()) {
            return session.getStartTime();
        }
        else {
            return getChronology(session).getZone().convertUTCToLocal(session.getStartTime());
        }
    }

    private static Optional<Domain> retentionToDefaultTimeFilter(ConnectorSession session, Optional<Type> timestampType, Optional<Duration> duration)
    {
        if (!duration.isPresent() || !timestampType.isPresent()) {
            return Optional.empty();
        }
        long sessionStartTimeInMs = getSessionStartTimeInMs(session);
        long retentionInstantInMs = getChronology(session).seconds().subtract(sessionStartTimeInMs, duration.get().roundTo(TimeUnit.SECONDS));
        long lowTimeSeconds = retentionInstantInMs / 1000; // round down
        long highTimeSeconds = (sessionStartTimeInMs + 999) / 1000; // round up

        return Optional.of(Domain.create(ValueSet.ofRanges(new Range(Marker.above(timestampType.get(), lowTimeSeconds), Marker.below(timestampType.get(), highTimeSeconds))), false));
    }

    /**
     * Convert the current context to a AresDB request (AQL)
     */
    public AugmentedAQL toAresDbRequest(Optional<List<AresDbColumnHandle>> columnHandles, Optional<AresDbConfig> aresDbConfig, Optional<ConnectorSession> session)
    {
        List<Selection> measures;
        List<Selection> dimensions;
        if (groupByColumns.isEmpty() && !hasAggregation()) {
            // simple selections
            measures = ImmutableList.of(Selection.of("1", Origin.LITERAL, BIGINT));
            dimensions = selections.values().stream().collect(Collectors.toList());
        }
        else if (!groupByColumns.isEmpty()) {
            measures = selections.entrySet().stream()
                    .filter(c -> !groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());

            dimensions = selections.entrySet().stream()
                    .filter(c -> groupByColumns.contains(c.getKey()))
                    .map(c -> c.getValue())
                    .collect(Collectors.toList());
        }
        else {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Ares does not handle non group by aggregation queries yet, either fix in Ares or add final aggregation to table " + tableHandle.getTableName());
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
                dimensionJson.put("timeUnit", "second");
            }
            dimensionsJson.add(dimensionJson);
        }

        request.put("table", tableHandle.getTableName());
        request.put("measures", measuresJson);
        request.put("dimensions", dimensionsJson);
        Optional<Domain> timeFilter = this.timeFilter.isPresent() ? this.timeFilter : session.flatMap(s -> retentionToDefaultTimeFilter(s, tableHandle.getTimeColumnType(), tableHandle.getRetention()));
        timeFilter.ifPresent(tf -> addTimeFilter(request, tableHandle.getTimeColumnName().get(), tf));

        if (!filters.isEmpty()) {
            JSONArray filterJson = new JSONArray();
            filters.forEach(filterJson::add);
            request.put("rowFilters", filterJson);
        }

        Long limit = this.limit;
        if (!hasAggregation()) {
            if (limit == null) {
                limit = -1L;
            }
            if (session.isPresent()) {
                // This is only safe to throw in the actual getNextPage (execution) code path.
                // Outside of that, it will simply disable the (partial) pushdown and lead to wrong planning
                // So we are using the presence of session as a proxy for being in the getNextPage code path
                // Creating a separate "ExecutionContext" class would be an overkill, but worth doing for later
                long maxLimit = aresDbConfig.map(AresDbConfig::getMaxLimitWithoutAggregates).orElse(-1L);
                if (maxLimit > 0 && (limit < 0 || limit > maxLimit)) {
                    throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Inferred limit %d (specified %d) is greater than max aresdb allowed limit of %d when no aggregates are present in table %s", limit, this.limit, maxLimit, tableHandle.getTableName()));
                }
            }
        }
        if (limit != null) {
            request.put("limit", limit);
        }

        JSONArray requestJoins = new JSONArray(joins.size());
        for (JoinInfo joinInfo : joins) {
            JSONObject join = new JSONObject();
            join.put("alias", joinInfo.alias);
            join.put("table", joinInfo.name);
            JSONArray conditions = new JSONArray();
            joinInfo.conditions.forEach(conditions::add);
            join.put("conditions", conditions);
            requestJoins.add(join);
        }
        if (!requestJoins.isEmpty()) {
            request.put("joins", requestJoins);
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

    private static void addTimeFilter(JSONObject request, String timeColumn, Domain timeFilter)
    {
        if (timeFilter.isAll()) {
            return;
        }
        Optional<String> from = Optional.empty();
        Optional<String> to = Optional.empty();
        ValueSet values = timeFilter.getValues();
        if (values.isSingleValue()) {
            from = objectToTimeLiteral(values.getSingleValue(), true);
            to = objectToTimeLiteral(values.getSingleValue(), false);
        }
        else if (values instanceof SortedRangeSet) {
            Range span = ((SortedRangeSet) values).getSpan();
            if (!span.getLow().isLowerUnbounded()) {
                from = objectToTimeLiteral(span.getLow().getValue(), true);
            }
            if (!span.getHigh().isUpperUnbounded()) {
                to = objectToTimeLiteral(span.getHigh().getValue(), false);
            }
        }
        if (!from.isPresent()) {
            throw new IllegalArgumentException(String.format("Expected to extract a from for the time column %s from the given domain filter %s", timeColumn, timeFilter));
        }
        JSONObject timeFilterJson = new JSONObject();
        timeFilterJson.put("column", timeColumn);
        timeFilterJson.put("from", from.get());
        to.ifPresent(s -> timeFilterJson.put("to", s));
        request.put("timeFilter", timeFilterJson);
    }

    private static Optional<String> objectToTimeLiteral(Object literal, boolean isLow)
    {
        if (!(literal instanceof Number)) {
            return Optional.empty();
        }
        double num = ((Number) literal).doubleValue();
        long numAsLong = (long) (isLow ? Math.floor(num) : Math.ceil(num));
        return Optional.of(Long.toString(numAsLong));
    }

    public AresDbQueryGeneratorContext withJoin(AresDbQueryGeneratorContext otherContext, String otherTableName, List<JoinPipelineNode.EquiJoinClause> criterias)
    {
        List<String> joinCriterias = criterias.stream().map(criteria -> {
            AresDbExpressionConverter.AresDbExpression left = criteria.getLeft().accept(new AresDbExpressionConverter(), selections);
            AresDbExpressionConverter.AresDbExpression right = criteria.getRight().accept(new AresDbExpressionConverter(), otherContext.selections);
            return format("%s = %s", left.getDefinition(), right.getDefinition());
        }).collect(toImmutableList());
        JoinInfo newJoinInfo = new JoinInfo(otherTableName, otherTableName, joinCriterias);
        AresDbQueryGeneratorContext ret = new AresDbQueryGeneratorContext(selections, tableHandle, timeFilter, filters, aggregationApplied, groupByColumns, limit, ImmutableList.<JoinInfo>builder().addAll(joins).add(newJoinInfo).build());
        if (otherContext.hasAggregation() || !otherContext.groupByColumns.isEmpty() || otherContext.hasLimit()) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Can only join with a right side that does not have limits nor aggregations");
        }
        ret = ret.withFilters(otherContext.filters, otherContext.timeFilter);
        LinkedHashMap<String, Selection> originalLeftSelections = ret.selections;
        LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>(originalLeftSelections);
        otherContext.selections.forEach((otherSymbol, otherSelection) -> {
            if (newSelections.containsKey(otherSymbol)) {
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, String.format("Found duplicate selection b/w left and right: %s. Left is %s, Right is %s", otherSymbol, originalLeftSelections, otherContext.selections));
            }
            newSelections.put(otherSymbol, otherSelection);
        });
        ret = ret.withProject(newSelections);
        return ret;
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

    public static class JoinInfo
    {
        private final String alias;
        private final String name;
        private final List<String> conditions;

        public JoinInfo(String alias, String name, List<String> conditions)
        {
            this.alias = alias;
            this.name = name;
            this.conditions = conditions;
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

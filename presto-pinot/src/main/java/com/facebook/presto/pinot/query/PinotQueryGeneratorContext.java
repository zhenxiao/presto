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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotErrorCode;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the components needed to construct a PQL query and provides methods to update the current context with new operations.
 */
class PinotQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<String, Selection> selections;
    private final LinkedHashSet<String> groupByColumns;
    private final String from;
    private final String filter;
    private final Optional<String> timeBoundaryFilter;
    private final Optional<Long> limit;
    private final int numAggregations;

    PinotQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, Optional<String> timeBoundaryFilter)
    {
        this(selections, from, null, timeBoundaryFilter, 0, new LinkedHashSet<>(), Optional.empty());
    }

    private PinotQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, String filter, Optional<String> timeBoundaryFilter,
            int numAggregations, LinkedHashSet<String> groupByColumns, Optional<Long> limit)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.from = requireNonNull(from, "source can't be null");
        this.numAggregations = numAggregations;
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available");
        this.filter = filter;
        this.timeBoundaryFilter = requireNonNull(timeBoundaryFilter, "timeBoundaryFilter can't be null");
        this.limit = limit;
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withFilter(String filter)
    {
        checkState(!hasFilter(), "There already exists a filter. Pinot doesn't support filters at multiple levels");
        checkState(!hasAggregation(), "Pinot doesn't support filtering the results of aggregation");
        checkState(!hasLimit(), "Pinot doesn't support filtering on top of the limit");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, numAggregations, groupByColumns, limit);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withAggregation(LinkedHashMap<String, Selection> newSelections, LinkedHashSet<String> groupByColumns, int numAggregations)
    {
        // there is only one aggregation supported.
        checkState(!hasAggregation(), "Pinot doesn't support aggregation on top of the aggregated data");
        checkState(!hasLimit(), "Pinot doesn't support aggregation on top of the limit");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, this.numAggregations + numAggregations, groupByColumns, limit);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withProject(LinkedHashMap<String, Selection> newSelections)
    {
        checkState(!hasAggregation(), "Pinot doesn't support new selections on top of the aggregated data");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, 0, new LinkedHashSet<>(), limit);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withLimit(long limit, boolean isPartial)
    {
        if (!isPartial) {
            // We support final limit only on top of the aggregated data.
            checkState(hasAggregation(), "Final limit pushdown is supported only on top of the aggregated data");
        }

        checkState(!hasLimit(), "Limit already exists. Pinot doesn't support limit on top of another limit");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, numAggregations, groupByColumns, Optional.of(limit));
    }

    private boolean hasFilter()
    {
        return filter != null;
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasAggregation()
    {
        return numAggregations > 0;
    }

    public LinkedHashMap<String, Selection> getSelections()
    {
        return selections;
    }

    /**
     * Convert the current context to a PQL
     */
    PinotQueryGenerator.GeneratedPql toQuery(Optional<List<PinotColumnHandle>> columnHandles, Optional<PinotConfig> pinotConfig, boolean forSinglePageSource)
    {
        if (pinotConfig.isPresent() && !pinotConfig.get().isAllowMultipleAggregations() && numAggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by is forbidden");
        }

        if (hasLimit() && numAggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by and limit is forbidden");
        }

        String exprs = selections.entrySet().stream()
                .filter(s -> !groupByColumns.contains(s.getKey())) // remove the group by columns from the query as Pinot barfs if the group by column is an expression
                .map(s -> s.getValue().getDefinition())
                .collect(Collectors.joining(", "));

        String query = "SELECT " + exprs + " FROM " + from;
        if (filter != null) {
            String finalFilter = filter;
            if (timeBoundaryFilter.isPresent()) {
                // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
                // at the same time add the timecolumn to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
                finalFilter = timeBoundaryFilter.get() + " AND " + filter + "";
            }
            query = query + " WHERE " + finalFilter;
        }

        if (!groupByColumns.isEmpty()) {
            String groupByExpr = groupByColumns.stream().map(x -> selections.get(x).getDefinition()).collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
        }

        // Rules for limit:
        // - If its a selection query:
        //      + given limit or configured limit
        // - Else if has group by:
        //      + ensure that only one aggregation
        //      + default limit or configured top limit
        // - Fail if limit is invalid

        String limitKeyWord = "";
        long limitLong = -1;
        long defaultLimit = pinotConfig.map(PinotConfig::getLimitLarge).orElse(PinotConfig.DEFAULT_LIMIT_LARGE);

        if (!hasAggregation()) {
            if (forSinglePageSource && pinotConfig.isPresent()) {
                long maxLimit = pinotConfig.get().getMaxSelectLimitWhenSinglePage();
                long givenLimit = limit.orElse(defaultLimit);
                if (givenLimit > maxLimit) {
                    throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), String.format("Given limit %d more than max select limit %d", givenLimit, maxLimit));
                }
            }
            limitLong = limit.orElse(defaultLimit);
            limitKeyWord = "LIMIT";
        }
        else if (!groupByColumns.isEmpty()) {
            limitKeyWord = "TOP";
            if (limit.isPresent()) {
                if (numAggregations > 1) {
                    throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.of(query),
                            String.format("Pinot has weird semantics with group by and multiple aggregation functions and limits"));
                }
                else {
                    limitLong = limit.get();
                }
            }
            else {
                limitLong = defaultLimit;
            }
        }

        if (!limitKeyWord.isEmpty()) {
            if (limitLong <= 0 || limitLong > Integer.MAX_VALUE) {
                throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Limit " + limitLong + " not supported: Limit is not being pushed down");
            }
            query += " " + limitKeyWord + " " + (int) limitLong;
        }

        return new PinotQueryGenerator.GeneratedPql(from, query, getIndicesMappingFromPinotSchemaToPrestoSchema(columnHandles, query), groupByColumns.size());
    }

    private Optional<List<Integer>> getIndicesMappingFromPinotSchemaToPrestoSchema(Optional<List<PinotColumnHandle>> columnHandles, String query)
    {
        LinkedHashMap<String, Selection> expressionsInOrder = new LinkedHashMap<>();
        for (String groupByColumn : groupByColumns) {
            Selection groupByColumnDefinition = selections.get(groupByColumn);
            if (groupByColumnDefinition == null) {
                throw new IllegalStateException(format("Group By column (%s) definition not found in input selections: ",
                        groupByColumn, Joiner.on(",").withKeyValueSeparator(":").join(selections)));
            }
            expressionsInOrder.put(groupByColumn, groupByColumnDefinition);
        }
        expressionsInOrder.putAll(selections);

        return columnHandles.map(handles -> {
            checkState(handles.size() == expressionsInOrder.size(), "Expected returned expressions %s to match column handles %s",
                    Joiner.on(",").withKeyValueSeparator(":").join(expressionsInOrder), Joiner.on(",").join(handles));
            Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < handles.size(); ++i) {
                String columnName = handles.get(i).getColumnName();
                Integer prev = nameToIndex.put(columnName.toLowerCase(ENGLISH), i);
                if (prev != null) {
                    throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected Pinot column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
                }
            }
            ImmutableList.Builder<Integer> outputIndices = ImmutableList.builder();
            for (Map.Entry<String, Selection> expression : expressionsInOrder.entrySet()) {
                Integer index = nameToIndex.get(expression.getKey());
                if (index == null) {
                    throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected to find a Pinot column handle for the expression %s, but we have %s",
                            expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
                }
                outputIndices.add(index);
            }
            return outputIndices.build();
        });
    }

    /**
     * Where is the selection/projection originated from
     */
    enum Origin
    {
        TABLE_COLUMN, // refers to direct column in table
        DERIVED, // expression is derived from one or more input columns or a combination of input columns and literals
        LITERAL, // derived from literal
    }

    // Projected/selected column definition in query
    static class Selection
    {
        private final String definition;
        private final Origin origin;
        private final Type dataType;

        Selection(String definition, Origin origin, Type dataType)
        {
            this.definition = definition;
            this.origin = origin;
            this.dataType = dataType;
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        public Type getDataType()
        {
            return dataType;
        }

        @Override
        public String toString()
        {
            return definition;
        }
    }
}

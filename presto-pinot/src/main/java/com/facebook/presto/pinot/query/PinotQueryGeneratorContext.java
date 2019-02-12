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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the components needed to construct a PQL query and provides methods to update the current context with new operations.
 */
class PinotQueryGeneratorContext
{
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<String, Selection> selections;
    private final List<String> groupByColumns;
    private final String from;
    private final String filter;
    private final Optional<String> timeBoundaryFilter;
    private final Long limit;
    private final boolean aggregationApplied;

    PinotQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, Optional<String> timeBoundaryFilter)
    {
        this(selections, from, null, timeBoundaryFilter, false, ImmutableList.of(), null);
    }

    private PinotQueryGeneratorContext(LinkedHashMap<String, Selection> selections, String from, String filter, Optional<String> timeBoundaryFilter,
            boolean aggregationApplied, List<String> groupByColumns, Long limit)
    {
        this.selections = requireNonNull(selections, "selections can't be null");
        this.from = requireNonNull(from, "source can't be null");
        this.aggregationApplied = aggregationApplied;
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
        checkArgument(!hasFilter(), "There already exists a filter. Pinot doesn't support filters at multiple levels");
        checkArgument(!hasAggregation(), "Pinot doesn't support filtering the results of aggregation");
        checkArgument(!hasLimit(), "Pinot doesn't support filtering on top of the limit");
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, aggregationApplied, groupByColumns, limit);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withAggregation(LinkedHashMap<String, Selection> newSelections, List<String> groupByColumns)
    {
        // there is only one aggregation supported.
        checkArgument(!hasAggregation(), "Pinot doesn't support aggregation on top of the aggregated data");
        checkArgument(!hasLimit(), "Pinot doesn't support aggregation on top of the limit");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, true, groupByColumns, limit);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withProject(LinkedHashMap<String, Selection> newSelections)
    {
        checkArgument(!hasAggregation(), "Pinot doesn't support new selections on top of the aggregated data");
        return new PinotQueryGeneratorContext(newSelections, from, filter, timeBoundaryFilter, false, ImmutableList.of(), limit);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    PinotQueryGeneratorContext withLimit(long limit)
    {
        return new PinotQueryGeneratorContext(selections, from, filter, timeBoundaryFilter, aggregationApplied, groupByColumns, limit);
    }

    private boolean hasFilter()
    {
        return filter != null;
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

    /**
     * Convert the current context to a PQL
     */
    PinotQueryGenerator.GeneratedPql toQuery(Optional<List<PinotColumnHandle>> columnHandles, Optional<PinotConfig> pinotConfig)
    {
        String exprs = selections.values().stream()
                .filter(c -> !groupByColumns.contains(c.definition)) // remove the group by columns from the query as Pinot barfs if the group by column is an expression
                .map(c -> c.definition)
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
            String groupByExpr = groupByColumns.stream().collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
        }

        if (limit != null) {
            query += " LIMIT " + limit;
        }
        else if (pinotConfig.isPresent()) {
            query += " LIMIT " + pinotConfig.get().getLimitLarge();
        }

        return new PinotQueryGenerator.GeneratedPql(query, getIndicesMappingFromPinotSchemaToPrestoSchema(columnHandles, query), groupByColumns.size());
    }

    /**
     * TODO: clean up this
     */
    private Optional<List<Integer>> getIndicesMappingFromPinotSchemaToPrestoSchema(Optional<List<PinotColumnHandle>> columnHandles, String query)
    {
        LinkedHashSet<String> expressionsInOrder = new LinkedHashSet<>();
        if (!groupByColumns.isEmpty()) {
            String groupByExpr = groupByColumns.stream().collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
            Map<String, String> colMappingReverse = selections.entrySet().stream().collect(Collectors.toMap(p -> p.getValue().getDefinition(), p -> p.getKey()));
            if (colMappingReverse.size() != selections.size()) {
                throw new IllegalStateException(format("Expected colMapping to be fully reversible: %s, for query %s", Joiner.on(",").withKeyValueSeparator(":").join(selections), query));
            }
            for (String groupByColumn : groupByColumns) {
                String outputGroupByColumn = colMappingReverse.get(groupByColumn);
                if (outputGroupByColumn == null) {
                    throw new IllegalStateException("Expected to find a presto column name for the given pinot group by column " + groupByColumn);
                }
                expressionsInOrder.add(outputGroupByColumn);
            }
            // Group by columns come first and have already been added above
            // so we are adding the metrics below
            expressionsInOrder.addAll(selections.keySet());
        }
        else {
            expressionsInOrder.addAll(selections.keySet());
        }

        Map<String, String> colMappingReverse = selections.entrySet().stream().collect(Collectors.toMap(p -> p.getValue().getDefinition(), p -> p.getKey()));
        if (colMappingReverse.size() != selections.size()) {
            throw new IllegalStateException(format("Expected colMapping to be fully reversible: %s, for query %s", Joiner.on(",").withKeyValueSeparator(":").join(selections), query));
        }
        expressionsInOrder.clear();
        for (String groupByColumn : groupByColumns) {
            String outputGroupByColumn = colMappingReverse.get(groupByColumn);
            if (outputGroupByColumn == null) {
                throw new IllegalStateException(format("Expected to find a presto column name for the given Pinot group by column " + groupByColumn));
            }
            expressionsInOrder.add(outputGroupByColumn);
        }
        // Group by columns come first
        expressionsInOrder.addAll(selections.keySet());

        return columnHandles.map(handles -> {
            Preconditions.checkState(handles.size() == expressionsInOrder.size(), "Expected returned expressions %s to match columm handles %s",
                    Joiner.on(",").join(expressionsInOrder), Joiner.on(",").join(handles));
            Map<String, Integer> nameToIndex = new HashMap<>();
            for (int i = 0; i < handles.size(); ++i) {
                String columnName = handles.get(i).getColumnName();
                Integer prev = nameToIndex.put(columnName, i);
                if (prev != null) {
                    throw new IllegalStateException(format("Expected Pinot column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
                }
            }
            ImmutableList.Builder<Integer> columnIndices = ImmutableList.builder();
            for (String expression : expressionsInOrder) {
                Integer index = nameToIndex.get(expression);
                if (index == null) {
                    throw new IllegalStateException(format("Expected to find a Pinot column handle for the expression %s, but we have %s",
                            expression, Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
                }
                columnIndices.add(index);
            }
            return columnIndices.build();
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

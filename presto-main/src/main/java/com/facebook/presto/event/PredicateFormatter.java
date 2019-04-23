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
package com.facebook.presto.event;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.connector.ConnectorId.isInternalSystemConnector;
import static com.facebook.presto.event.PredicateFormatter.RangeValue.MAX_INFINITY;
import static com.facebook.presto.event.PredicateFormatter.RangeValue.MIN_INFINITY;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.castToVarchar;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PredicateFormatter
{
    private PredicateFormatter() {}

    private static final JsonCodec<ColumnPredicate> columnPredicateJsonCodec = JsonCodec.jsonCodec(ColumnPredicate.class);

    public static List<String> getColumnPredicates(StageInfo outputStageInfo, FunctionManager functionRegistry, Session session)
    {
        ImmutableList.Builder<ColumnPredicate> builder = ImmutableList.builder();
        List<PlanFragment> planFragments = getAllStages(Optional.of(outputStageInfo)).stream().map(StageInfo::getPlan).filter(Objects::nonNull).collect(toImmutableList());
        for (PlanFragment fragment : planFragments) {
            TableScanVisitor visitor = new TableScanVisitor(functionRegistry, session);
            fragment.getRoot().accept(visitor, null);
            builder.addAll(visitor.getPredicates());
        }
        return builder.build().stream().map(columnPredicateJsonCodec::toJson).collect(toImmutableList());
    }

    private static class TableScanVisitor
            extends PlanVisitor<Void, Void>
    {
        private final FunctionManager functionRegistry;
        private final Session session;

        private final ImmutableList.Builder<ColumnPredicate> columnPredicateBuilder = ImmutableList.builder();

        public List<ColumnPredicate> getPredicates()
        {
            return columnPredicateBuilder.build();
        }

        public TableScanVisitor(FunctionManager functionRegistry, Session session)
        {
            this.functionRegistry = requireNonNull(functionRegistry);
            this.session = requireNonNull(session);
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TupleDomain<? extends ColumnHandle> predicate = node.getCompactEffectiveConstraint();
            if (!predicate.isNone() && !predicate.isAll()) {
                Map<? extends ColumnHandle, Domain> columnHandleDomainMap = predicate.getDomains().get();
                columnHandleDomainMap.entrySet().stream().forEach((columnHandleDomainEntry -> {
                    ColumnHandle column = columnHandleDomainEntry.getKey();
                    // TODO decide whether to use domain.simplify
                    List<PredicateValue> predicateValues = formatDomain(columnHandleDomainEntry.getValue().simplify());
                    if (!isInternalSystemConnector(node.getTable().getConnectorId()) && !predicateValues.isEmpty()) {
                        columnPredicateBuilder.add(new ColumnPredicate(node.getTable().getConnectorHandle(), column, predicateValues));
                    }
                }));
            }
            return null;
        }

        // Transfer domain to human readable json
        // Domain use Block, which is not readable and for internal usage only
        private List<PredicateValue> formatDomain(Domain domain)
        {
            ImmutableList.Builder<PredicateValue> parts = ImmutableList.builder();
            Type type = domain.getType();
            domain.getValues().getValuesProcessor().consume(
                    ranges -> {
                        for (Range range : ranges.getOrderedRanges()) {
                            if (range.isSingleValue()) {
                                String value = castToVarchar(type, range.getSingleValue(), functionRegistry, session);
                                parts.add(new PredicateValue(value, null));
                            }
                            else {
                                String min;
                                if (range.getLow().isLowerUnbounded()) {
                                    min = MIN_INFINITY;
                                }
                                else {
                                    min = castToVarchar(type, range.getLow().getValue(), functionRegistry, session);
                                }

                                String max;
                                if (range.getHigh().isUpperUnbounded()) {
                                    max = MAX_INFINITY;
                                }
                                else {
                                    max = castToVarchar(type, range.getHigh().getValue(), functionRegistry, session);
                                }
                                parts.add(new PredicateValue(null, new RangeValue(min, max)));
                            }
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> castToVarchar(type, value, functionRegistry, session))
                            .sorted()
                            .map(value -> new PredicateValue(value, null))
                            .forEach(parts::add),
                    allOrNone -> {
                        // ignore
                    });

            return parts.build();
        }
    }

    @Immutable
    public static class ColumnPredicate
    {
        private final ConnectorTableHandle tableHandle;
        private final ColumnHandle columnHandle;
        private final List<PredicateValue> predicates;

        @JsonCreator
        public ColumnPredicate(@JsonProperty("tableHandle") ConnectorTableHandle tableHandle, @JsonProperty("columnHandle") ColumnHandle columnHandle, @JsonProperty("predicates") List<PredicateValue> predicates)
        {
            this.tableHandle = tableHandle;
            this.columnHandle = columnHandle;
            this.predicates = predicates;
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public ColumnHandle getColumnHandle()
        {
            return columnHandle;
        }

        @JsonProperty
        public List<PredicateValue> getPredicates()
        {
            return predicates;
        }
    }

    @Immutable
    public static class PredicateValue
    {
        private final String singleValue;
        private final RangeValue rangeValue;

        @JsonCreator
        public PredicateValue(@Nullable @JsonProperty("singleValue") String singleValue, @Nullable @JsonProperty("rangeValue") RangeValue rangeValue)
        {
            this.singleValue = singleValue;
            this.rangeValue = rangeValue;
        }

        @Nullable
        @JsonProperty
        public String getSingleValue()
        {
            return singleValue;
        }

        @Nullable
        @JsonProperty
        public RangeValue getRangeValue()
        {
            return rangeValue;
        }
    }

    @Immutable
    public static class RangeValue
    {
        public static final String MIN_INFINITY = "MIN_INFINITY";
        public static final String MAX_INFINITY = "MAX_INFINITY";

        private final String min;
        private final String max;

        @JsonCreator
        public RangeValue(@JsonProperty("min") String min, @JsonProperty("max") String max)
        {
            this.min = min;
            this.max = max;
        }

        @JsonProperty
        public String getMin()
        {
            return min;
        }

        @JsonProperty
        public String getMax()
        {
            return max;
        }
    }
}

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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.StandardErrorCode.NO_PARTITION_FILTER;
import static com.facebook.presto.sql.planner.optimizations.MetadataQueryOptimizer.getTableLayout;
import static java.util.Objects.requireNonNull;

public class EnforcePartitionFilter
        implements PlanOptimizer
{
    private static final String ERROR_MESSAGE_FORMAT = "Your query is missing partition filter. Please %s %s in the WHERE clause of your query. For example: WHERE partition_column > '2017-06-01'.";
    private final Metadata metadata;
    private Set<SchemaTableName> partitionFilterEnforcedTables;

    public EnforcePartitionFilter(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
        this.partitionFilterEnforcedTables = ImmutableSet.of();
    }

    private static Set<SchemaTableName> getSchemaTableNames(String tableList)
    {
        Spliterator<String> spliterator = Splitter.on(':').omitEmptyStrings().trimResults().split(tableList).spliterator();
        return StreamSupport.stream(spliterator, false).map(SchemaTableName::valueOf).collect(Collectors.toSet());
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        String partitionFilterTables = SystemSessionProperties.getPartitionFilterTables(session);
        if (!SystemSessionProperties.enforcePartitionFilter(session) || partitionFilterTables.isEmpty()) {
            return plan;
        }
        this.partitionFilterEnforcedTables = getSchemaTableNames(partitionFilterTables);
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, this.partitionFilterEnforcedTables, idAllocator), plan, new PartitionFilteringContext());
    }

    private static class PartitionFilteringContext
    {
        private boolean isExplainAnalyze;

        public boolean isExplainAnalyze()
        {
            return isExplainAnalyze;
        }

        public void setExplainAnalyze(boolean explainAnalyze)
        {
            isExplainAnalyze = explainAnalyze;
        }
    }

    private static class Optimizer
            extends SimplePlanRewriter<PartitionFilteringContext>
    {
        private final Session session;
        private final Metadata metadata;
        private final Set<SchemaTableName> partitionFilteringTables;

        private Optimizer(Session session, Metadata metadata, Set<SchemaTableName> tables, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.metadata = metadata;
            this.partitionFilteringTables = tables;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<PartitionFilteringContext> context)
        {
            if (context.get().isExplainAnalyze()) {
                return context.defaultRewrite(tableScan);
            }

            TableLayout layout = getTableLayout(metadata, session, tableScan);
            if (layout == null || !layout.getDiscretePredicates().isPresent()) {
                return context.defaultRewrite(tableScan);
            }

            SchemaTableName schemaTableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
            if (!partitionFilteringTables.contains(schemaTableName)) {
                return context.defaultRewrite(tableScan);
            }

            Set<String> partitionColumns = layout.getDiscretePredicates().get().getColumns().stream().map(columnHandle -> metadata
                    .getColumnMetadata(session, tableScan.getTable(), columnHandle).getName()).collect(Collectors.toSet());
            Set<String> predicateColumns = getPredicateColumns(tableScan.getOriginalConstraint());
            boolean containsAllPartitionColumns = partitionColumns.stream().allMatch(partitionColumn -> predicateColumns.stream().anyMatch(predicate -> predicate.startsWith(partitionColumn)));
            if (!containsAllPartitionColumns) {
                String partitionColumnNames = partitionColumns.stream().map(str -> "\"" + str + "\"").collect(Collectors.joining(", "));
                if (partitionColumns.size() == 1) {
                    throw new PrestoException(NO_PARTITION_FILTER, String.format(ERROR_MESSAGE_FORMAT, "add a filter on the partition column", partitionColumnNames));
                }
                else {
                    throw new PrestoException(NO_PARTITION_FILTER, String.format(ERROR_MESSAGE_FORMAT, "add filters on all partition columns", partitionColumnNames));
                }
            }
            return context.defaultRewrite(tableScan);
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<PartitionFilteringContext> context)
        {
            context.get().setExplainAnalyze(true);
            return super.visitExplainAnalyze(node, context);
        }

        private Set<String> getPredicateColumns(Expression predicate)
        {
            Set<String> symbols = new HashSet<>();
            ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void>
                        treeRewriter)
                {
                    symbols.add(node.getName().toLowerCase());
                    return super.rewriteSymbolReference(node, context, treeRewriter);
                }
            }, predicate);
            return symbols;
        }
    }
}

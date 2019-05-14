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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class EnforcePartitionFilter
        implements PlanOptimizer
{
    private static final String ERROR_MESSAGE_FORMAT =
            "Your query is missing partition column filters. Please add filters on partition "
            + "columns ('%s') for table '%s' in the WHERE clause of your query. For example: "
            + "WHERE DATE(%s) > CURRENT_DATE - INTERVAL '7' DAY. See more details at "
            + "https://engdocs.uberinternal.com/sql-analytics-guide/presto_pages/optimization.html#partition-column.";
    private static final Pattern DATE_COLUMN_NAME_PATTERN = Pattern.compile(".*(date|week|month).*");
    private final Metadata metadata;

    public EnforcePartitionFilter(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    private static Set<SchemaTableName> getSchemaTableNames(String tableList)
    {
        Spliterator<String> spliterator = Splitter.on(':').omitEmptyStrings().trimResults().split(tableList).spliterator();
        return StreamSupport.stream(spliterator, false).map(SchemaTableName::valueOf).collect(Collectors.toSet());
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        String partitionFilterTables = SystemSessionProperties.getPartitionFilterTables(session);
        if (!SystemSessionProperties.enforcePartitionFilter(session) || partitionFilterTables.isEmpty()) {
            return plan;
        }
        Set<SchemaTableName> enforcedTables = getSchemaTableNames(partitionFilterTables);
        Set<String> enforcedSchemas = enforcedTables.stream().filter(schemaTableName -> "*".equals(schemaTableName.getTableName())).map(SchemaTableName::getSchemaName).collect(Collectors.toSet());
        PartitionFilteringContext context = new PartitionFilteringContext();
        plan.accept(new Visitor(), context);
        Map<PlanNodeId, TableScanInfo> tableScanInfos = context.getTableScanInfos();
        for (TableScanInfo tableScanInfo : tableScanInfos.values()) {
            checkTableScan(tableScanInfo, session, enforcedTables, enforcedSchemas);
        }
        return plan;
    }

    private void checkTableScan(TableScanInfo tableScanInfo, Session session, Set<SchemaTableName> enforcedTables, Set<String> enforcedSchemas)
    {
        TableScanNode tableScan = tableScanInfo.getTableScanNode();
        TableLayout layout = getTableLayout(metadata, session, tableScan);
        if (layout == null || !layout.getDiscretePredicates().isPresent()) {
            return;
        }
        SchemaTableName schemaTableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
        if (!enforcedSchemas.contains(schemaTableName.getSchemaName()) && !enforcedTables.contains(schemaTableName)) {
            return;
        }
        List<ColumnHandle> partitionColumns = layout.getDiscretePredicates().get().getColumns();
        Set<ColumnHandle> predicateColumns = ImmutableSet.<ColumnHandle>builder()
                .addAll(getColumnHandleInFilterPredicate(tableScanInfo))
                .addAll(getColumnHandleFromEnforcedConstraint(tableScan)).build();

        if (!predicateColumns.containsAll(partitionColumns)) {
            Set<String> partitionColumnNames = new TreeSet<>();
            String dateColumnName = "datestr";
            for (ColumnHandle handle : partitionColumns) {
                String name = metadata.getColumnMetadata(session, tableScan.getTable(), handle).getName();
                partitionColumnNames.add(name);
                if (DATE_COLUMN_NAME_PATTERN.matcher(name).matches()) {
                    dateColumnName = name;
                }
            }
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format(ERROR_MESSAGE_FORMAT,
                    String.join("', '", partitionColumnNames), schemaTableName, dateColumnName));
        }
    }

    private Set<ColumnHandle> getColumnHandleInFilterPredicate(TableScanInfo tableScanInfo)
    {
        if (!tableScanInfo.getPredicates().isPresent()) {
            return ImmutableSet.of();
        }
        TableScanNode tableScan = tableScanInfo.getTableScanNode();
        Set<Symbol> references = SymbolsExtractor.extractUnique(tableScanInfo.getPredicates().get());
        return references.stream()
                .filter(symbol -> tableScan.getAssignments().containsKey(symbol))
                .map(symbol -> tableScan.getAssignments().get(symbol))
                .collect(toImmutableSet());
    }

    private Set<ColumnHandle> getColumnHandleFromEnforcedConstraint(TableScanNode tableScanNode)
    {
        Set<ColumnHandle> symbols = new HashSet<>();
        tableScanNode.getEnforcedConstraint().getDomains().map(Map::keySet).ifPresent(symbols::addAll);
        return symbols;
    }

    public static TableLayout getTableLayout(Metadata metadata, Session session, TableScanNode tableScan)
    {
        TableLayout layout = null;
        if (!tableScan.getLayout().isPresent()) {
            List<TableLayoutResult> layouts = metadata.getLayouts(session, tableScan.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty());
            if (layouts.size() == 1) {
                layout = Iterables.getOnlyElement(layouts).getLayout();
            }
        }
        else {
            layout = metadata.getLayout(session, tableScan.getLayout().get());
        }
        return layout;
    }

    private static class PartitionFilteringContext
    {
        private Map<PlanNodeId, TableScanInfo> tableScanInfos = new HashMap<>();
        private boolean isExplainAnalyze;

        boolean isExplainAnalyze()
        {
            return isExplainAnalyze;
        }

        void setExplainAnalyze(boolean explainAnalyze)
        {
            isExplainAnalyze = explainAnalyze;
        }

        Map<PlanNodeId, TableScanInfo> getTableScanInfos()
        {
            return tableScanInfos;
        }
    }

    private static class TableScanInfo
    {
        private final TableScanNode tableScanNode;
        private final Optional<Expression> predicates;

        TableScanInfo(TableScanNode tableScanNode, Optional<Expression> predicate)
        {
            this.tableScanNode = requireNonNull(tableScanNode);
            this.predicates = requireNonNull(predicate);
        }

        TableScanNode getTableScanNode()
        {
            return tableScanNode;
        }

        Optional<Expression> getPredicates()
        {
            return predicates;
        }
    }

    private static class Visitor
            extends SimplePlanVisitor<PartitionFilteringContext>
    {
        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, PartitionFilteringContext context)
        {
            context.setExplainAnalyze(true);
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, PartitionFilteringContext context)
        {
            if (context.isExplainAnalyze()) {
                return null;
            }
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode child = (TableScanNode) node.getSource();
                Preconditions.checkArgument(!context.getTableScanInfos().containsKey(child.getId()), "tableScan already exists before checking filter node");
                context.getTableScanInfos().put(child.getId(), new TableScanInfo(child, Optional.of(node.getPredicate())));
            }
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode tableScan, PartitionFilteringContext context)
        {
            if (context.isExplainAnalyze()) {
                return null;
            }
            context.getTableScanInfos().putIfAbsent(tableScan.getId(), new TableScanInfo(tableScan, Optional.empty()));
            return null;
        }
    }
}

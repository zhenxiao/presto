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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.type.JsonType;
import com.facebook.presto.util.JsonUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GeoSpatialQueryRewriter
        implements PlanOptimizer
{
    private static final String JSON_ARRAY_GET = "json_array_get";
    private static final String JSON_FORMAT = "json_format";
    private static final String BUILD_GEO_INDEX = "build_geo_index";
    private static final String ST_GEOMETRY_TO_TEXT = "st_geometry_to_text";
    private static final String GEO_CONTAINS_ALL_SYMBOL = "geo_contains_all_symbol";
    private static final String UNNEST_OUTPUT_SYMBOL = "unnest_symbol";

    public enum GeoOperator
    {
        CONTAINS("st_contains", "geo_contains_all", "geo_contains_all_with_shape"),
        INTERSECT("st_intersects", "geo_intersects_all", "geo_intersects_all_with_shape");

        final String operator;
        final String getAllFunction;
        final String getAllWithShapeFunction;

        GeoOperator(String operator, String getAllFunction, String getAllWithShapeFunction)
        {
            this.operator = operator;
            this.getAllFunction = getAllFunction;
            this.getAllWithShapeFunction = getAllWithShapeFunction;
        }
    }

    private final Metadata metadata;

    public GeoSpatialQueryRewriter(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.rewriteGeoSpatialQuery(session)) {
            return plan;
        }
        return SimplePlanRewriter.rewriteWith(new Rewriter(metadata, idAllocator, symbolAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(Metadata metadata, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.metadata = requireNonNull(metadata, "idAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            if (!(node.getSource() instanceof FilterNode)) {
                return context.defaultRewrite(node);
            }
            FilterNode filterNode = (FilterNode) node.getSource();
            Expression filterNodePredicate = filterNode.getPredicate();
            Optional<GeoFunction> geoFunction = GeoFunction.from(filterNodePredicate);
            if (!geoFunction.isPresent()) {
                return context.defaultRewrite(node);
            }
            Optional<JoinNode> crossJoinNode = getCrossJoinNode(filterNode);
            if (!crossJoinNode.isPresent()) {
                return context.defaultRewrite(node);
            }
            GeoIndexNodeBuilder geoIndexNodeBuilder = createGeoIndexNodeBuilder(crossJoinNode.get(), geoFunction.get());
            if (!geoIndexNodeBuilder.canBuildAggregationNode()) {
                return context.defaultRewrite(node);
            }
            // Create aggregation node on top of geo data side
            AggregationNode aggregationNode = geoIndexNodeBuilder.createAggregationNode();
            checkArgument(aggregationNode.getOutputSymbols().size() == 1, "aggregation node should has only one output");

            // Create join node to join the new aggregation node and old read data side
            Symbol aggregationOutputSymbol = aggregationNode.getOutputSymbols().get(0);
            JoinNode newJoinNode = createNewJoinNode(crossJoinNode.get(), geoIndexNodeBuilder.getReadDataSide(), aggregationNode);

            // create filter node with the new join node as source
            FunctionCall geoContainsFunctionCall = geoFunction.get().createGetAllIndexFunction(aggregationOutputSymbol.toSymbolReference());
            FilterNode newFilterNode = new FilterNode(idAllocator.getNextId(), newJoinNode, new IsNotNullPredicate(geoContainsFunctionCall));

            // create unnestNode to expand [columns, array(t)] to [columns,  t]
            UnnestNode unnestNode = getUnnestNode(node, geoFunction, geoIndexNodeBuilder,
                    aggregationOutputSymbol, newFilterNode);

            ImmutableList<Symbol> unnestSymbols = ImmutableList.<Symbol>builder().addAll(Iterables.concat(unnestNode
                    .getUnnestSymbols().values())).build();
            Preconditions.checkArgument(unnestSymbols.size() == 1, "UnnestNode has more than 1 unnest symbols");

            // create project node applying geo_contains_all function
            Expression oldPredicateWithoutGeoOperator = geoFunction.get().createPredicateWithoutGeoOperation(filterNodePredicate);
            Set<Symbol> symbols = SymbolsExtractor.extractUnique(oldPredicateWithoutGeoOperator);
            Set<Symbol> originalOutputSymbols = SymbolsExtractor.extractUniqueNonRecursive(node);
            Assignments assignments = getProjectAssignments(Sets.union(symbols, originalOutputSymbols).stream().collect(Collectors.toList()), geoIndexNodeBuilder, unnestSymbols.get(0).toSymbolReference());
            ProjectNode newProjectNode = new ProjectNode(idAllocator.getNextId(), unnestNode, assignments);

            // apply old predicates except geo operator
            FilterNode filterNodeWithOldPredicates = new FilterNode(idAllocator.getNextId(), newProjectNode, oldPredicateWithoutGeoOperator);
            return new ProjectNode(idAllocator.getNextId(), filterNodeWithOldPredicates, node.getAssignments());
        }

        private UnnestNode getUnnestNode(ProjectNode node, Optional<GeoFunction> geoFunction, GeoIndexNodeBuilder geoIndexNodeBuilder, Symbol aggregationOutputSymbol, FilterNode newFilterNode)
        {
            List<Symbol> replicatedSymbols = newFilterNode.getOutputSymbols().stream().filter(symbol -> !symbol.equals(aggregationOutputSymbol)).collect(Collectors.toList());

            Assignments.Builder projectAssigments = Assignments.builder();
            projectAssigments.putIdentities(replicatedSymbols);

            Type geoContainsAllReturnType = isShapeIncluded(node, geoIndexNodeBuilder) ? GeoContainsAllShapeFunction.RETURN_TYPE : VarcharType.VARCHAR;
            Symbol geoContainsAllSymbol = symbolAllocator.newSymbol(GEO_CONTAINS_ALL_SYMBOL, new ArrayType(geoContainsAllReturnType));
            Symbol geoContainsUnnestSymbol = symbolAllocator.newSymbol(UNNEST_OUTPUT_SYMBOL, geoContainsAllReturnType);

            if (geoContainsAllReturnType.equals(VarcharType.VARCHAR)) {
                projectAssigments.put(geoContainsAllSymbol, geoFunction.get().createGetAllIndexFunction(aggregationOutputSymbol.toSymbolReference()));
            }
            else {
                projectAssigments.put(geoContainsAllSymbol, geoFunction.get().createGetAllIndexWithShapeFunction(aggregationOutputSymbol.toSymbolReference()));
            }

            ProjectNode projectAfterFilter = new ProjectNode(idAllocator.getNextId(), newFilterNode, projectAssigments.build());
            return new UnnestNode(idAllocator.getNextId(), projectAfterFilter, replicatedSymbols, ImmutableMap.of(geoContainsAllSymbol, ImmutableList.of(geoContainsUnnestSymbol)), Optional.empty());
        }

        private boolean isShapeIncluded(ProjectNode projectNode, GeoIndexNodeBuilder geoIndexNodeBuilder)
        {
            Symbol geoShapeSymbol = geoIndexNodeBuilder.getGeoShapeSymbol();
            Set<Symbol> originalOutputSymbols = SymbolsExtractor.extractUniqueNonRecursive(projectNode);
            return originalOutputSymbols.contains(geoShapeSymbol);
        }

        private Assignments getProjectAssignments(List<Symbol> originalOutputSymbols, GeoIndexNodeBuilder geoIndexNodeBuilder, Expression row)
        {
            Assignments.Builder builder = Assignments.builder();
            // get all old output symbols
            Symbol geoShapeSymbol = geoIndexNodeBuilder.getGeoShapeSymbol();
            Set<Symbol> oldOutputSymbolsWithoutShape = originalOutputSymbols.stream().filter(it -> !it.equals(geoShapeSymbol)).collect(Collectors.toSet());

            if (originalOutputSymbols.contains(geoShapeSymbol)) {
                Expression getGeoShapeFunction = GeoContainsAllShapeFunction.getShapeExpression(row);
                row = GeoContainsAllShapeFunction.getIndexExpression(row);
                Type geoShapeType = symbolAllocator.getTypes().get(geoShapeSymbol);
                if (geoShapeType.equals(VarbinaryType.VARBINARY)) {
                    builder.put(geoShapeSymbol, getGeoShapeFunction);
                }
                else {
                    checkArgument(geoShapeType.equals(VarcharType.VARCHAR), "geo shape geoShapeType is " + geoShapeType + " but not varchar");
                    FunctionCall convertShapeToTextFunctionCall = new FunctionCall(QualifiedName.of(ST_GEOMETRY_TO_TEXT), ImmutableList.of(getGeoShapeFunction));
                    builder.put(geoShapeSymbol, convertShapeToTextFunctionCall);
                }
            }

            // symbols needed in final output that can be fetched from geo_index
            Set<Symbol> geoDataOutputSymbols = geoIndexNodeBuilder.getGeoDataOutputSymbols().stream().collect(Collectors.toSet());
            Set<Symbol> geoIndexSymbols = Sets.intersection(oldOutputSymbolsWithoutShape, geoDataOutputSymbols);
            Set<Symbol> identitySymbols = Sets.difference(oldOutputSymbolsWithoutShape, geoIndexSymbols);
            builder.putIdentities(identitySymbols);

            if (geoIndexSymbols.size() == 1) {
                // Need to cast indexed field to old type
                Symbol oldOutputSymbol = geoIndexSymbols.iterator().next();
                String oldOutputSymbolType = symbolAllocator.getTypes().get(oldOutputSymbol).getDisplayName();
                builder.put(oldOutputSymbol, new Cast(row, oldOutputSymbolType));
            }
            else if (geoIndexSymbols.size() > 1) {
                QualifiedName jsonArrayGet = QualifiedName.of(JSON_ARRAY_GET);
                List<Symbol> geoDataOutputList = geoDataOutputSymbols.stream().collect(Collectors.toList());
                for (Symbol symbol : geoIndexSymbols) {
                    int index = geoDataOutputList.indexOf(symbol);
                    checkArgument(index >= 0, "index must exist in geoDataOutputSymbol");
                    String symbolType = symbolAllocator.getTypes().get(symbol).getDisplayName();
                    Expression valueNode = new GenericLiteral(StandardTypes.BIGINT, String.valueOf(index));
                    FunctionCall jsonArrayGetFunction = new FunctionCall(jsonArrayGet, ImmutableList.of(row, valueNode));

                    FunctionCall jsonFormatFunction = new FunctionCall(QualifiedName.of(JSON_FORMAT), ImmutableList.of(jsonArrayGetFunction));
                    builder.put(symbol, new Cast(jsonFormatFunction, symbolType));
                }
            }
            return builder.build();
        }

        private Optional<JoinNode> getCrossJoinNode(FilterNode filterNode)
        {
            if (isCrossJoinNode(filterNode.getSource())) {
                return Optional.of((JoinNode) filterNode.getSource());
            }
            else if (filterNode.getSource() instanceof ProjectNode && isCrossJoinNode(((ProjectNode) filterNode.getSource()).getSource())) {
                return Optional.of((JoinNode) ((ProjectNode) filterNode.getSource()).getSource());
            }
            return Optional.empty();
        }

        private boolean isCrossJoinNode(PlanNode node)
        {
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                return joinNode.isCrossJoin();
            }
            return false;
        }

        private JoinNode createNewJoinNode(JoinNode joinNode, PlanNode readDataNode, AggregationNode aggregationNode)
        {
            ImmutableList<Symbol> outputSymbols = ImmutableList.<Symbol>builder().addAll(readDataNode.getOutputSymbols()).addAll
                    (aggregationNode.getOutputSymbols()).build();
            return new JoinNode(idAllocator.getNextId(), joinNode.getType(), readDataNode, aggregationNode, joinNode.getCriteria(), outputSymbols, Optional.empty(), Optional.empty(), Optional.empty(), joinNode.getDistributionType());
        }

        private GeoIndexNodeBuilder createGeoIndexNodeBuilder(JoinNode joinNode, GeoFunction geoFunction)
        {
            Symbol shapeSymbol = geoFunction.getShapeSymbol();
            PlanNode rightNode = joinNode.getRight();
            PlanNode leftNode = joinNode.getLeft();
            if (rightNode.getOutputSymbols().contains(shapeSymbol)) {
                return new GeoIndexNodeBuilder(rightNode, leftNode, geoFunction);
            }
            else {
                return new GeoIndexNodeBuilder(leftNode, rightNode, geoFunction);
            }
        }

        private class GeoIndexNodeBuilder
        {
            private final PlanNode geoIndexSide;
            private final PlanNode readDataSide;
            private final GeoIndexAggregation geoIndexAggregation;

            private GeoIndexNodeBuilder(PlanNode geoIndexSide, PlanNode readDataSide, GeoFunction geoFunction)
            {
                this.geoIndexSide = geoIndexSide;
                this.readDataSide = readDataSide;
                this.geoIndexAggregation = createGeoIndexContext(geoIndexSide, geoFunction);
            }

            private GeoIndexAggregation createGeoIndexContext(PlanNode geoDataNode, GeoFunction geoFunction)
            {
                // TODO Right now just assume expression in funciton is symbolReference
                // TODO Actually rightNodeSymbol could be simplefied_shape and geoFunctionalPrameter shapeSymbol could be st_text_to_binary(simplified_shape)
                Symbol shapeSymbol = geoFunction.getShapeSymbol();
                List<Symbol> allOutputSymbols = geoDataNode.getOutputSymbols();
                List<Symbol> outputSymbols = allOutputSymbols.stream().filter(symbol -> !symbol.equals(shapeSymbol)).collect(Collectors.toList());
                return new GeoIndexAggregation(shapeSymbol, outputSymbols);
            }

            public List<Symbol> getGeoDataOutputSymbols()
            {
                return geoIndexAggregation.getOutputSymbols();
            }

            public AggregationNode createAggregationNode()
            {
                return geoIndexAggregation.getAggregationNode(geoIndexSide);
            }

            public PlanNode getReadDataSide()
            {
                return readDataSide;
            }

            public Symbol getGeoShapeSymbol()
            {
                return geoIndexAggregation.getGeoShapeSymbol();
            }

            public boolean canBuildAggregationNode()
            {
                Map<Symbol, Type> types = symbolAllocator.getTypes();
                return geoIndexAggregation.getOutputSymbols().stream().map(symbol -> types.get(symbol)).allMatch(JsonUtil::canCastToJson);
            }
        }

        public class GeoIndexAggregation
        {
            private final Symbol geoShapeSymbol;
            private final List<Symbol> outputSymbols;

            GeoIndexAggregation(Symbol geoShapeSymbol, List<Symbol> outputSymbols)
            {
                this.geoShapeSymbol = geoShapeSymbol;
                this.outputSymbols = outputSymbols;
            }

            public Symbol getGeoShapeSymbol()
            {
                return geoShapeSymbol;
            }

            public List<Symbol> getOutputSymbols()
            {
                return outputSymbols;
            }

            private AggregationNode getAggregationNode(PlanNode planNode, Optional<Symbol> indexSymbol)
            {
                List<Symbol> buildGeoIndexSymbols = indexSymbol.isPresent() ? ImmutableList.of(indexSymbol.get(), geoShapeSymbol) : ImmutableList.of(geoShapeSymbol);
                Symbol buildGeoIndexFunctionSymbol = symbolAllocator.newSymbol(BUILD_GEO_INDEX, VarcharType.VARCHAR);
                Signature signature = metadata.getFunctionRegistry().resolveFunction(QualifiedName.of(BUILD_GEO_INDEX),
                        buildGeoIndexSymbols.stream().map(symbol -> new TypeSignatureProvider(symbolAllocator.getTypes().get
                                (symbol).getTypeSignature())).collect(Collectors.toList()));
                List<Expression> symbols = buildGeoIndexSymbols.stream().map(symbol -> symbol.toSymbolReference()).collect(Collectors.toList());
                AggregationNode.Aggregation buildGeoIndexAggregation = new AggregationNode.Aggregation(new FunctionCall(QualifiedName.of(BUILD_GEO_INDEX), symbols), signature, Optional.empty());
                Map<Symbol, AggregationNode.Aggregation> aggregationMap = ImmutableMap.of(buildGeoIndexFunctionSymbol, buildGeoIndexAggregation);
                return new AggregationNode(idAllocator.getNextId(), planNode, aggregationMap, ImmutableList.of(ImmutableList.of()), AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());
            }

            public AggregationNode getAggregationNode(PlanNode planNode)
            {
                if (outputSymbols.isEmpty()) {
                    return getAggregationNode(planNode, Optional.empty());
                }
                else if (outputSymbols.size() == 1) {
                    return getAggregationNode(planNode, Optional.of(outputSymbols.get(0)));
                }
                Expression row = new Row(outputSymbols.stream().map(Symbol::toSymbolReference).collect(Collectors.toList()));
                Expression cast = new Cast(row, StandardTypes.JSON);
                Symbol combinedOutputSymbol = symbolAllocator.newSymbol(cast, JsonType.JSON);
                Assignments.Builder builder = Assignments.builder();
                builder.putIdentity(geoShapeSymbol);
                builder.put(combinedOutputSymbol, cast);
                return getAggregationNode(new ProjectNode(idAllocator.getNextId(), planNode, builder.build()), Optional.of(combinedOutputSymbol));
            }
        }
    }

    static class GeoFunction
    {
        private static final String ST_CONTAINS = "st_contains";
        private static final String ST_INTERSECTS = "st_intersects";

        private final SymbolReference container;
        private final Expression object;
        private final GeoOperator geoOperator;

        public GeoFunction(FunctionCall geospatialFunction)
        {
            checkArgument(geospatialFunction.getArguments().size() == 2);
            Expression expression = geospatialFunction.getArguments().get(0);
            checkArgument(expression instanceof SymbolReference);
            this.container = (SymbolReference) expression;
            this.object = geospatialFunction.getArguments().get(1);
            this.geoOperator = QualifiedName.of(ST_CONTAINS).equals(geospatialFunction.getName()) ? GeoOperator.CONTAINS : GeoOperator.INTERSECT;
        }

        private static boolean isGeoOperator(QualifiedName name)
        {
            return name.equals(QualifiedName.of(ST_CONTAINS)) || name.equals(QualifiedName.of(ST_INTERSECTS));
        }

        public static Optional<GeoFunction> from(Expression filterNodePredicate)
        {
            List<FunctionCall> stContainsFuctionCalls = new ArrayList<>();
            new DefaultExpressionTraversalVisitor<Void, List<FunctionCall>>()
            {
                @Override
                protected Void visitFunctionCall(FunctionCall node, List<FunctionCall> geospatialFunctions)
                {
                    QualifiedName functionName = node.getName();
                    if (isGeoOperator(functionName)) {
                        checkArgument(node.getArguments().size() == 2, "st_contains or st_intersects should have two parameters");
                        geospatialFunctions.add(node);
                    }
                    return null;
                }
            }.process(filterNodePredicate, stContainsFuctionCalls);

            // Only support one geospatial function and the shape argument is a symbol reference
            if (stContainsFuctionCalls.size() == 1 && stContainsFuctionCalls.get(0).getArguments().get(0) instanceof SymbolReference) {
                return Optional.ofNullable(new GeoFunction(stContainsFuctionCalls.get(0)));
            }
            return Optional.empty();
        }

        FunctionCall createGetAllIndexFunction(Expression outputSymbol)
        {
            return new FunctionCall(QualifiedName.of(geoOperator.getAllFunction), ImmutableList.of(object, outputSymbol));
        }

        FunctionCall createGetAllIndexWithShapeFunction(Expression outputSymbol)
        {
            return new FunctionCall(QualifiedName.of(geoOperator.getAllWithShapeFunction), ImmutableList.of(object, outputSymbol));
        }

        Expression createPredicateWithoutGeoOperation(Expression predicate)
        {
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void>
                        treeRewriter)
                {
                    if (isGeoOperator(node.getName())) {
                        return BooleanLiteral.TRUE_LITERAL;
                    }
                    return super.rewriteFunctionCall(node, context, treeRewriter);
                }
            }, predicate);
        }

        public Symbol getShapeSymbol()
        {
            return Symbol.from(container);
        }
    }

    public static class GeoContainsAllShapeFunction
    {
        //public static final Type RETURN_TYPE = new RowType(ImmutableList.of(VarcharType.VARCHAR, VarbinaryType.VARBINARY), Optional.of(ImmutableList.of("field0", "field1")));
        public static final Type RETURN_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of("field0"), VarcharType.VARCHAR), new RowType.Field(Optional.of("field1"), VarbinaryType.VARBINARY)));

        public static Expression getIndexExpression(Expression input)
        {
            return new DereferenceExpression(input, new Identifier("field0"));
        }

        public static Expression getShapeExpression(Expression input)
        {
            return new DereferenceExpression(input, new Identifier("field1"));
        }
    }
}

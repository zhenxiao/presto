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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PushDownExpressionGenerator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static java.util.Locale.ENGLISH;

public class PinotTestUtils
{
    private PinotTestUtils()
    {
    }

    // This method is copied from PlanBuilder in presto-main module. Pinot doesn't depend upon the presto-main module.
    public static Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(AS_DOUBLE)));
    }

    // This method is copied from PlanBuilder in presto-main module. Pinot doesn't depend upon the presto-main module.
    public static Expression rewriteIdentifiersToSymbolReferences(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }
        }, expression);
    }

    public static PushDownExpression pdExpr(String sqlExpr)
    {
        Expression expression = PinotTestUtils.expression(sqlExpr);
        // Assume all types are integers, because we don't have access to the full machinery here to do type inference
        return new PushDownExpressionGenerator((expr) -> BIGINT).process(expression);
    }

    public static PipelineNode filter(PushDownExpression predicate, List<String> outputColumns, List<Type> rowType)
    {
        return new FilterPipelineNode(predicate, outputColumns, rowType, Optional.empty(), Optional.empty());
    }

    public static PipelineNode project(List<PushDownExpression> expressions, List<String> outputColumns, List<Type> rowType)
    {
        return new ProjectPipelineNode(expressions, outputColumns, rowType);
    }

    public static PipelineNode scan(ConnectorTableHandle tableHandle, List<ColumnHandle> inputColumns)
    {
        List<String> columnNames = inputColumns.stream().map(c -> ((PinotColumnHandle) c).getColumnName().toLowerCase(ENGLISH)).collect(Collectors.toList());
        return scan(tableHandle, inputColumns, columnNames);
    }

    public static PipelineNode scan(ConnectorTableHandle tableHandle, List<ColumnHandle> inputColumns, List<String> outputColumnNames)
    {
        List<Type> rowType = inputColumns.stream().map(c -> ((PinotColumnHandle) c).getDataType()).collect(Collectors.toList());
        return new TablePipelineNode(tableHandle, inputColumns, outputColumnNames, rowType);
    }

    public static PipelineNode agg(List<AggregationPipelineNode.Node> nodes, boolean partial)
    {
        return new AggregationPipelineNode(nodes, partial);
    }

    public static LimitPipelineNode limit(long limit, boolean partial, List<String> outputColumns, List<Type> rowType)
    {
        return new LimitPipelineNode(limit, partial, outputColumns, rowType);
    }

    public static TableScanPipeline pipeline(PipelineNode... nodes)
    {
        List<PipelineNode> pipelineNodes = Arrays.asList(nodes);
        PipelineNode lastPipelineNode = pipelineNodes.get(pipelineNodes.size() - 1);
        List<ColumnHandle> columnHandles;
        if (lastPipelineNode instanceof TablePipelineNode) {
            columnHandles = ((TablePipelineNode) lastPipelineNode).getInputColumns();
        }
        else {
            columnHandles = PinotMetadata.createDerivedColumnHandles(lastPipelineNode);
        }
        return new TableScanPipeline(pipelineNodes, columnHandles);
    }

    public static List<ColumnHandle> columnHandles(ColumnHandle... cols)
    {
        return Arrays.asList(cols);
    }

    public static List<String> cols(String... cols)
    {
        return Arrays.asList(cols);
    }

    public static List<Type> types(Type... types)
    {
        return Arrays.asList(types);
    }
}

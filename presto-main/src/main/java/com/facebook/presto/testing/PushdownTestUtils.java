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
package com.facebook.presto.testing;

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
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PushDownExpressionGenerator;
import com.facebook.presto.sql.tree.Expression;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class PushdownTestUtils
{
    private PushdownTestUtils()
    {
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql, new ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL)));
    }

    public static PushDownExpression pdExpr(String sqlExpr)
    {
        Expression expression = expression(sqlExpr);
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

    public static PipelineNode scan(ConnectorTableHandle tableHandle, List<ColumnHandle> inputColumns, List<String> outputColumnNames, List<Type> rowType)
    {
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

    public static TableScanPipeline pipeline(Function<PipelineNode, List<ColumnHandle>> derivedHandlesCreator, PipelineNode... nodes)
    {
        List<PipelineNode> pipelineNodes = Arrays.asList(nodes);
        PipelineNode lastPipelineNode = pipelineNodes.get(pipelineNodes.size() - 1);
        List<ColumnHandle> columnHandles;
        if (lastPipelineNode instanceof TablePipelineNode) {
            columnHandles = ((TablePipelineNode) lastPipelineNode).getInputColumns();
        }
        else {
            columnHandles = derivedHandlesCreator.apply(lastPipelineNode);
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

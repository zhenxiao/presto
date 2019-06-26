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
package com.facebook.presto.spi.pipeline;

public abstract class TableScanPipelineVisitor<R, C>
{
    public R visitNode(PipelineNode node, C context)
    {
        throw new UnsupportedOperationException("Unsupported pipeline node: " + String.valueOf(node));
    }

    public R visitTableNode(TablePipelineNode table, C context)
    {
        return visitNode(table, context);
    }

    public R visitProjectNode(ProjectPipelineNode project, C context)
    {
        return visitNode(project, context);
    }

    public R visitFilterNode(FilterPipelineNode filter, C context)
    {
        return visitNode(filter, context);
    }

    public R visitAggregationNode(AggregationPipelineNode aggregation, C context)
    {
        return visitNode(aggregation, context);
    }

    public R visitLimitNode(LimitPipelineNode limit, C context)
    {
        return visitNode(limit, context);
    }

    public R visitSortNode(SortPipelineNode sort, C context)
    {
        return visitNode(sort, context);
    }

    public R visitJoinNode(JoinPipelineNode joinPipelineNode, C context)
    {
        return visitNode(joinPipelineNode, context);
    }
}

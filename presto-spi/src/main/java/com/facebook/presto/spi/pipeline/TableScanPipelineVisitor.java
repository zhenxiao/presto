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
        throw new UnsupportedOperationException("Unsupported node type: " + node.getType());
    }

    public abstract R visitTableNode(TablePipelineNode table, C context);

    public abstract R visitProjectNode(ProjectPipelineNode project, C context);

    public abstract R visitFilterNode(FilterPipelineNode filter, C context);

    public abstract R visitAggregationNode(AggregationPipelineNode aggregation, C context);

    public abstract R visitLimitNode(LimitPipelineNode limit, C context);

    public abstract R visitSortNode(SortPipelineNode limit, C context);
}

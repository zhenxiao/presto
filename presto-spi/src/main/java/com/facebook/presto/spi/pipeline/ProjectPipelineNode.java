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

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ProjectPipelineNode
        extends PipelineNode
{
    private final List<PushDownExpression> exprs;
    private final List<String> outputColumns;
    private final List<Type> rowType;

    @JsonCreator
    public ProjectPipelineNode(
            @JsonProperty("exprs") List<PushDownExpression> exprs,
            @JsonProperty("outputColumns") List<String> outputColumns,
            @JsonProperty("rowType") List<Type> rowType)
    {
        this.exprs = requireNonNull(exprs, "exprs is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
    }

    @Override
    public PipelineType getType()
    {
        return PipelineType.PROJECT;
    }

    @Override
    @JsonProperty
    public List<String> getOutputColumns()
    {
        return outputColumns;
    }

    @Override
    @JsonProperty
    public List<Type> getRowType()
    {
        return rowType;
    }

    @JsonProperty
    public List<PushDownExpression> getExprs()
    {
        return exprs;
    }

    @Override
    public String toString()
    {
        return "Project: " + exprs.stream().map(e -> e.toString()).collect(Collectors.joining(","));
    }

    @Override
    public <R, C> R accept(TableScanPipelineVisitor<R, C> visitor, C context)
    {
        return visitor.visitProjectNode(this, context);
    }
}

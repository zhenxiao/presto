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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AggregationPipelineNode
        extends PipelineNode
{
    private final List<Node> nodes;
    private final boolean partial;

    @JsonCreator
    public AggregationPipelineNode(@JsonProperty("nodes") List<Node> nodes, @JsonProperty("partial") boolean partial)
    {
        this.nodes = requireNonNull(nodes, "nodes is null");
        this.partial = partial;
    }

    public AggregationPipelineNode(boolean partial)
    {
        this.nodes = new ArrayList<>();
        this.partial = partial;
    }

    public void addAggregation(List<String> inputColumns, String function, String outputColumn, Type outputType)
    {
        nodes.add(new Aggregation(inputColumns, function, outputColumn, outputType));
    }

    public void addGroupBy(String inputColumn, String outputColumn, Type type)
    {
        nodes.add(new GroupByColumn(inputColumn, outputColumn, type));
    }

    @Override
    public PipelineType getType()
    {
        return partial ? PipelineType.AGGREGATE_PARTIAL : PipelineType.AGGREGATE_FINAL;
    }

    @JsonProperty("nodes")
    public List<Node> getNodes()
    {
        return nodes;
    }

    @JsonProperty("partial")
    public boolean isPartial()
    {
        return partial;
    }

    @Override
    public List<String> getOutputColumns()
    {
        return nodes.stream().map(Node::getOutputColumn).collect(Collectors.toList());
    }

    @Override
    public List<Type> getRowType()
    {
        return nodes.stream().map(Node::getOutputType).collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(TableScanPipelineVisitor<R, C> visitor, C context)
    {
        return visitor.visitAggregationNode(this, context);
    }

    @Override
    public String toString()
    {
        return "Aggregation:" + nodes.stream().map(f -> f.toString()).collect(Collectors.joining(","));
    }

    public enum ExprType
    {
        GROUP_BY,
        AGGREGATE,
    }

    /**
     * Group by field description
     */
    public static class GroupByColumn
            extends Node
    {
        private final String inputColumn;

        @JsonCreator
        public GroupByColumn(
                @JsonProperty("inputColumn") String inputColumn,
                @JsonProperty("outputColumn") String outputColumn,
                @JsonProperty("outputType") Type outputType)
        {
            super(ExprType.GROUP_BY, outputColumn, outputType);
            this.inputColumn = inputColumn;
        }

        @JsonProperty
        public String getInputColumn()
        {
            return inputColumn;
        }

        @Override
        public String toString()
        {
            return inputColumn;
        }
    }

    /**
     * Agg function description.
     */
    public static class Aggregation
            extends Node
    {
        private final List<String> inputColumns;
        private final String function;

        @JsonCreator
        public Aggregation(
                @JsonProperty("inputs") final List<String> inputs,
                @JsonProperty("function") final String function,
                @JsonProperty("outputColumn") final String output,
                @JsonProperty("outputType") final Type outputType)
        {
            super(ExprType.AGGREGATE, output, outputType);
            this.inputColumns = requireNonNull(inputs, "inputs is null");
            this.function = requireNonNull(function, "function is null");
        }

        @JsonProperty("function")
        public String getFunction()
        {
            return function;
        }

        @JsonProperty("inputs")
        public List<String> getInputs()
        {
            return inputColumns;
        }

        @Override
        public String toString()
        {
            return function + "(" + inputColumns.stream().collect(Collectors.joining(",")) + ")";
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Aggregation.class, name = "aggregation"),
            @JsonSubTypes.Type(value = GroupByColumn.class, name = "groupby")})
    public static class Node
    {
        private final ExprType exprType;
        private final String outputColumn;
        private final Type outputType;

        @JsonCreator
        public Node(ExprType exprType, String outputColumn, Type outputType)
        {
            this.exprType = exprType;
            this.outputColumn = outputColumn;
            this.outputType = outputType;
        }

        @JsonProperty("outputColumn")
        public String getOutputColumn()
        {
            return outputColumn;
        }

        @JsonProperty("outputType")
        public Type getOutputType()
        {
            return outputType;
        }

        @JsonProperty("exprType")
        public ExprType getExprType()
        {
            return exprType;
        }
    }
}

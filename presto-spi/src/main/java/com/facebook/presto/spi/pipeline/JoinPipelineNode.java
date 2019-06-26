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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JoinPipelineNode
        extends PipelineNode
{
    private final Optional<PushDownExpression> filter;
    private final List<String> outputColumns;
    private final List<Type> rowType;
    private final JoinType joinType;
    private final ConnectorTableHandle other;
    private final Optional<TableScanPipeline> otherPipeline;
    private final List<EquiJoinClause> criteria;

    @JsonCreator
    public JoinPipelineNode(
            @JsonProperty("filter") Optional<PushDownExpression> filter,
            @JsonProperty("outputColumns") List<String> outputColumns,
            @JsonProperty("rowType") List<Type> rowType,
            @JsonProperty("other") ConnectorTableHandle other,
            @JsonProperty("otherPipeline") Optional<TableScanPipeline> otherPipeline,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("joinType") JoinType joinType)
    {
        this.filter = requireNonNull(filter);
        this.outputColumns = requireNonNull(outputColumns);
        this.rowType = requireNonNull(rowType);
        this.joinType = requireNonNull(joinType);
        this.other = requireNonNull(other);
        this.criteria = requireNonNull(criteria);
        this.otherPipeline = requireNonNull(otherPipeline);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", JoinPipelineNode.class.getSimpleName() + "[", "]")
                .add("filter=" + filter)
                .add("outputColumns=" + outputColumns)
                .add("rowType=" + rowType)
                .add("joinType=" + joinType)
                .add("other=" + other)
                .add("otherPipeline=" + otherPipeline)
                .add("criteria=" + criteria)
                .toString();
    }

    @JsonProperty
    public Optional<PushDownExpression> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public JoinType getJoinType()
    {
        return joinType;
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
    public ConnectorTableHandle getOther()
    {
        return other;
    }

    @JsonProperty
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty
    public Optional<TableScanPipeline> getOtherPipeline()
    {
        return otherPipeline;
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;
    }

    public static class EquiJoinClause
    {
        private final PushDownExpression left;
        private final PushDownExpression right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") PushDownExpression left, @JsonProperty("right") PushDownExpression right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty("left")
        public PushDownExpression getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public PushDownExpression getRight()
        {
            return right;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || !this.getClass().equals(obj.getClass())) {
                return false;
            }

            EquiJoinClause other = (EquiJoinClause) obj;

            return Objects.equals(this.left, other.left) &&
                    Objects.equals(this.right, other.right);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(left, right);
        }

        @Override
        public String toString()
        {
            return format("[JOIN] %s = %s", left, right);
        }
    }

    @Override
    public <R, C> R accept(TableScanPipelineVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoinNode(this, context);
    }
}

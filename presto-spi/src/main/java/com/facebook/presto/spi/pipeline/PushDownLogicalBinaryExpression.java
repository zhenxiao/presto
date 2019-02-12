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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PushDownLogicalBinaryExpression
        extends PushDownExpression
{
    private final PushDownExpression left;
    private final String operator;
    private final PushDownExpression right;

    @JsonCreator
    public PushDownLogicalBinaryExpression(
            @JsonProperty("left") PushDownExpression left,
            @JsonProperty("operator") String operator,
            @JsonProperty("right") PushDownExpression right)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @JsonProperty
    public String getOperator()
    {
        return operator;
    }

    @JsonProperty
    public PushDownExpression getLeft()
    {
        return left;
    }

    @JsonProperty
    public PushDownExpression getRight()
    {
        return right;
    }

    @Override
    public String toString()
    {
        return "( " + left + " " + operator + " " + right + " )";
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLogicalBinary(this, context);
    }
}

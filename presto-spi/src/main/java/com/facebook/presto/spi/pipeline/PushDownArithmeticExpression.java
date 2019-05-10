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

import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PushDownArithmeticExpression
        extends PushDownExpression
{
    private final PushDownExpression left;
    private final String operator;
    private final PushDownExpression right;

    @JsonCreator
    public PushDownArithmeticExpression(@JsonProperty("type") TypeSignature type, @JsonProperty("left") PushDownExpression left, @JsonProperty("operator") String operator, @JsonProperty("right") PushDownExpression right)
    {
        super(type);
        this.operator = operator;
        this.left = left;
        this.right = right;
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
        // This should be only used for debugging
        return String.format("(%s) %s (%s)", left, operator, right);
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitArithmeticExpression(this, context);
    }
}

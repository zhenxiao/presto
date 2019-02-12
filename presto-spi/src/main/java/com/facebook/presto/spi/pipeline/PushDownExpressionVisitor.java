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

public abstract class PushDownExpressionVisitor<R, C>
{
    public R visitExpression(PushDownExpression expression, C context)
    {
        throw new UnsupportedOperationException();
    }

    public R visitInputColumn(PushDownInputColumn inputColumn, C context)
    {
        return visitExpression(inputColumn, context);
    }

    public R visitFunction(PushDownFunction function, C context)
    {
        return visitExpression(function, context);
    }

    public R visitLogicalBinary(PushDownLogicalBinaryExpression comparision, C context)
    {
        return visitExpression(comparision, context);
    }

    public R visitInExpression(PushDownInExpression in, C context)
    {
        return visitExpression(in, context);
    }

    public R visitLiteral(PushDownLiteral literal, C context)
    {
        return visitExpression(literal, context);
    }

    public R visitBetweenExpression(PushDownBetweenExpression between, C context)
    {
        return visitExpression(between, context);
    }

    public R visitCastExpression(PushDownCastExpression cast, C context)
    {
        return visitExpression(cast, context);
    }
}

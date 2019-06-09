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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.pipeline.PushDownArithmeticExpression;
import com.facebook.presto.spi.pipeline.PushDownBetweenExpression;
import com.facebook.presto.spi.pipeline.PushDownCastExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownExpressionVisitor;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.PushDownNotExpression;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link PushDownExpression} in filter into Pinot complaint expression text
 */
class PinotFilterExpressionConverter
        extends PushDownExpressionVisitor<PinotExpression, Map<String, Selection>>
{
    private static final Set<String> LOGICAL_BINARY_OPS_FILTER = ImmutableSet.of("AND", "OR", "=", "<", "<=", ">", ">=", "<>");

    @Override
    public PinotExpression visitInputColumn(PushDownInputColumn inputColumn, Map<String, Selection> context)
    {
        Selection input = requireNonNull(context.get(inputColumn.getName()), format("Input column %s does not exist in the input", inputColumn.getName()));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    @Override
    public PinotExpression visitFunction(PushDownFunction function, Map<String, Selection> context)
    {
        // functions are not supported in filter expressions
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported in filter", function.getName()));
    }
    @Override
    public PinotExpression visitLogicalBinary(PushDownLogicalBinaryExpression logical, Map<String, Selection> context)
    {
        if (!LOGICAL_BINARY_OPS_FILTER.contains(logical.getOperator().toUpperCase(ENGLISH))) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("'%s' is not supported in filter", logical.getOperator()));
        }

        return derived(format("(%s %s %s)",
                logical.getLeft().accept(this, context).getDefinition(), logical.getOperator(), logical.getRight().accept(this, context).getDefinition()));
    }

    @Override
    public PinotExpression visitInExpression(PushDownInExpression in, Map<String, Selection> context)
    {
        return derived(format("(%s %s (%s))",
                in.getValue().accept(this, context).getDefinition(),
                in.isWhiteList() ? "IN" : "NOT IN",
                in.getArguments().stream()
                        .map(a -> a.accept(this, context).getDefinition())
                        .collect(Collectors.joining(", "))));
    }

    @Override
    public PinotExpression visitBetweenExpression(PushDownBetweenExpression between, Map<String, Selection> context)
    {
        return derived(format("(%s BETWEEN %s AND %s)",
                between.getValue().accept(this, context).getDefinition(),
                between.getLeft().accept(this, context).getDefinition(),
                between.getRight().accept(this, context).getDefinition()));
    }

    @Override
    public PinotExpression visitNotExpression(PushDownNotExpression not, Map<String, Selection> context)
    {
        PushDownExpression input = not.getInput();
        if (input instanceof PushDownInExpression) {
            // NOT operator is only supported on top of the IN expression
            PushDownInExpression in = (PushDownInExpression) input;
            in = new PushDownInExpression(in.getType(), !in.isWhiteList(), in.getValue(), in.getArguments());

            return in.accept(this, context);
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("NOT operator is supported only on top of IN operator. Received: ", input));
    }

    @Override
    public PinotExpression visitLiteral(PushDownLiteral literal, Map<String, Selection> context)
    {
        return new PinotExpression(literal.toString(), Origin.LITERAL);
    }

    @Override
    public PinotExpression visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        if (cast.isImplicit()) {
            return cast.getInput().accept(this, context);
        }
        else {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
        }
    }

    @Override
    public PinotExpression visitArithmeticExpression(PushDownArithmeticExpression expression, Map<String, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Arithmetic expressions are not supported in filter: " + expression.getOperator());
    }
}

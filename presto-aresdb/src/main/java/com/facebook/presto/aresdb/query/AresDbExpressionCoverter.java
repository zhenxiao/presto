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

package com.facebook.presto.aresdb.query;

import com.facebook.presto.aresdb.AresDbException;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin;
import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.query.AresDbExpressionCoverter.AresDbExpression.derived;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.LITERAL;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbExpressionCoverter
        extends PushDownExpressionVisitor<AresDbExpressionCoverter.AresDbExpression, Map<String, Selection>>
{
    public static class AresDbExpression
    {
        private final String definition;
        private final Optional<String> timeBucketizer;
        private final Origin origin;

        public AresDbExpression(String definition, Optional<String> timeBucketizer, Origin origin)
        {
            this.definition = requireNonNull(definition, "definition is null");
            this.timeBucketizer = requireNonNull(timeBucketizer, "timeBucketizer is null");
            this.origin = requireNonNull(origin, "origin is null");
        }

        public String getDefinition()
        {
            return definition;
        }

        public Optional<String> getTimeBucketizer()
        {
            return timeBucketizer;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        public static AresDbExpression derived(String definition)
        {
            return new AresDbExpression(definition, Optional.empty(), DERIVED);
        }

        public static AresDbExpression derived(String definition, String timeBucketizer)
        {
            return new AresDbExpression(definition, Optional.of(timeBucketizer), DERIVED);
        }
    }

    @Override
    public AresDbExpression visitInputColumn(PushDownInputColumn inputColumn, Map<String, Selection> context)
    {
        Selection input = requireNonNull(context.get(inputColumn.getName()), format("Input column %s does not exist in the input", inputColumn.getName()));
        return new AresDbExpression(input.getDefinition(), Optional.empty(), input.getOrigin());
    }

    @Override
    public AresDbExpression visitFunction(PushDownFunction function, Map<String, Selection> context)
    {
        switch (function.getName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                return handleDateTrunc(function, context);
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, format("function %s not supported yet", function.getName()));
        }
    }

    private AresDbExpression handleDateTrunc(PushDownFunction function, Map<String, Selection> context)
    {
        PushDownExpression timeInputParameter = function.getInputs().get(1);
        if (!(timeInputParameter instanceof PushDownFunction)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "input parameter to date_trunc is expected to be a function: " + function);
        }

        String inputColumn;

        PushDownFunction timeConversion = (PushDownFunction) timeInputParameter;
        switch (timeConversion.getName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                String column;
                if (timeConversion.getInputs().get(0) instanceof PushDownInputColumn) {
                    PushDownInputColumn pushdownInputColumn = (PushDownInputColumn) timeConversion.getInputs().get(0);
                    column = pushdownInputColumn.accept(this, context).getDefinition();
                    String columnName = pushdownInputColumn.getName();
                    if (!columnName.equalsIgnoreCase(column)) {
                        // AresDb can only handle the direct column input to `dateTimeConvert` function
                        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "unsupported expr: " + function);
                    }
                }
                else if (timeConversion.getInputs().get(0) instanceof PushDownCastExpression) {
                    PushDownCastExpression pushdownInputColumn = (PushDownCastExpression) timeConversion.getInputs().get(0);
                    column = pushdownInputColumn.accept(this, context).getDefinition();
                }
                else {
                    throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "input to time conversion function is not supported type: " + timeConversion.getInputs().get(0));
                }

                inputColumn = column;
                break;
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "not supported: " + timeConversion.getName());
        }

        PushDownExpression intervalParameter = function.getInputs().get(0);
        if (!(intervalParameter instanceof PushDownLiteral)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        PushDownLiteral intervalUnit = (PushDownLiteral) intervalParameter;
        switch (intervalUnit.getStrValue()) {
            case "second":
            case "minute":
            case "hour":
            case "day":
            case "week":
            case "month":
            case "quarter":
            case "year":
                break;
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "interval in date_trunc is not supported: " + intervalUnit.getStrValue());
        }

        return derived(inputColumn, intervalUnit.getStrValue());
    }

    @Override
    public AresDbExpression visitLogicalBinary(PushDownLogicalBinaryExpression comparision, Map<String, Selection> context)
    {
        return derived(format("(%s %s %s)",
                comparision.getLeft().accept(this, context).getDefinition(),
                comparision.getOperator(),
                comparision.getRight().accept(this, context).getDefinition()));
    }

    @Override
    public AresDbExpression visitArithmeticExpression(PushDownArithmeticExpression expression, Map<String, Selection> context)
    {
        return super.visitArithmeticExpression(expression, context);
    }

    @Override
    public AresDbExpression visitInExpression(PushDownInExpression in, Map<String, Selection> context)
    {
        return derived(format("(%s IN (%s))",
                in.getValue().accept(this, context).definition,
                in.getArguments().stream()
                        .map(a -> a.accept(this, context).definition)
                        .collect(Collectors.joining(", "))));
    }

    @Override
    public AresDbExpression visitLiteral(PushDownLiteral literal, Map<String, Selection> context)
    {
        return new AresDbExpression(literal.toString(), Optional.empty(), LITERAL);
    }

    @Override
    public AresDbExpression visitBetweenExpression(PushDownBetweenExpression between, Map<String, Selection> context)
    {
        String column = between.getValue().accept(this, context).definition;
        return derived(format("((%s >= %s) AND (%s <= %s))",
                column, between.getLeft().accept(this, context).definition,
                column, between.getRight().accept(this, context).definition));
    }

    @Override
    public AresDbExpression visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        return cast.getInput().accept(this, context);
    }

    @Override
    public AresDbExpression visitNotExpression(PushDownNotExpression not, Map<String, Selection> context)
    {
        return super.visitNotExpression(not, context);
    }
}

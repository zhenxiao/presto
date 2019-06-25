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
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.aresdb.AresDbErrorCode.ARESDB_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.aresdb.query.AresDbExpressionConverter.AresDbExpression.derived;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Origin.LITERAL;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbExpressionConverter
        extends PushDownExpressionVisitor<AresDbExpressionConverter.AresDbExpression, Map<String, Selection>>
{
    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

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

    private PushDownFunction getExpressionAsFunction(PushDownExpression originalExpression, PushDownExpression expression, Map<String, Selection> context)
    {
        if (expression instanceof PushDownFunction) {
            return (PushDownFunction) expression;
        }
        else if (expression instanceof PushDownCastExpression) {
            PushDownCastExpression castExpression = ((PushDownCastExpression) expression);
            if (isImplicitCast(castExpression)) {
                return getExpressionAsFunction(originalExpression, castExpression.getInput(), context);
            }
        }
        throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }

    private AresDbExpression handleDateTrunc(PushDownFunction function, Map<String, Selection> context)
    {
        PushDownExpression timeInputParameter = function.getInputs().get(1);
        timeInputParameter = getExpressionAsFunction(timeInputParameter, timeInputParameter, context);
        if (!(timeInputParameter instanceof PushDownFunction)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "input parameter to date_trunc is expected to be a function: " + function);
        }

        String inputColumn;

        PushDownFunction timeConversion = (PushDownFunction) timeInputParameter;
        switch (timeConversion.getName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                inputColumn = timeConversion.getInputs().get(0).accept(this, context).getDefinition();
                break;
            default:
                throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "not supported: " + timeConversion.getName());
        }

        PushDownExpression intervalParameter = function.getInputs().get(0);
        if (!(intervalParameter instanceof PushDownLiteral)) {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        PushDownLiteral intervalUnit = (PushDownLiteral) intervalParameter;
        switch (intervalUnit.getStrValue().toLowerCase(ENGLISH)) {
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
        AresDbExpression right = expression.getRight().accept(this, context);
        if (expression.getLeft() == null) {
            // unary ...
            String prefix = expression.getOperator().equals("-") ? "-" : "";
            return derived(prefix + right.getDefinition());
        }

        AresDbExpression left = expression.getLeft().accept(this, context);
        String prestoOp = expression.getOperator();

        return derived(format("%s %s %s", left.getDefinition(), prestoOp, right.getDefinition()));
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

    private static boolean isImplicitCast(PushDownCastExpression cast)
    {
        return cast.isImplicitCast(Optional.of((resultType, inputTypeSignature) -> Objects.equals(StandardTypes.TIMESTAMP, resultType) && TIME_EQUIVALENT_TYPES.contains(inputTypeSignature.getBase())));
    }

    @Override
    public AresDbExpression visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        if (isImplicitCast(cast)) {
            return cast.getInput().accept(this, context);
        }
        else {
            throw new AresDbException(ARESDB_UNSUPPORTED_EXPRESSION, "Non implicit casts not supported: " + cast);
        }
    }

    @Override
    public AresDbExpression visitNotExpression(PushDownNotExpression not, Map<String, Selection> context)
    {
        return derived("! " + not.getInput().accept(this, context).getDefinition());
    }
}

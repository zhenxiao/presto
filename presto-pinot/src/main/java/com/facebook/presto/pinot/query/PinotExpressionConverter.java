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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotExpressionConverter.PinotExpression.derived;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link PushDownExpression} into Pinot complaint expression text
 */
class PinotExpressionConverter
        extends PushDownExpressionVisitor<PinotExpressionConverter.PinotExpression, Map<String, Selection>>
{
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OP = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");

    @Override
    public PinotExpression visitInputColumn(PushDownInputColumn inputColumn, Map<String, Selection> context)
    {
        Selection input = requireNonNull(context.get(inputColumn.getName()), format("Input column %s does not exist in the input", inputColumn.getName()));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    @Override
    public PinotExpression visitFunction(PushDownFunction function, Map<String, Selection> context)
    {
        switch (function.getName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                return handleDateTrunc(function, context);
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported yet", function.getName()));
        }
    }

    private PinotExpression handleDateTrunc(PushDownFunction function, Map<String, Selection> context)
    {
        // Convert SQL standard function `DATE_TRUNC(INTERVAL, DATE/TIMESTAMP COLUMN)` to
        // Pinot's equivalent function `dateTimeConvert(columnName, inputFormat, outputFormat, outputGranularity)`
        // Pinot doesn't have a DATE/TIMESTAMP type. That means the input column (second argument) has been converted from numeric type to DATE/TIMESTAMP using one of the
        // conversion functions in SQL. First step is find the function and find its input column units (seconds, secondsSinceEpoch etc.)
        PushDownExpression timeInputParameter = function.getInputs().get(1);
        if (!(timeInputParameter instanceof PushDownFunction)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "input parameter to date_trunc is expected to be a function: " + function);
        }

        String inputColumn;
        String inputFormat;

        PushDownFunction timeConversion = (PushDownFunction) timeInputParameter;
        switch (timeConversion.getName().toLowerCase(ENGLISH)) {
            case "from_unixtime":
                String column;
                if (timeConversion.getInputs().get(0) instanceof PushDownInputColumn) {
                    PushDownInputColumn pushdownInputColumn = (PushDownInputColumn) timeConversion.getInputs().get(0);
                    column = pushdownInputColumn.accept(this, context).getDefinition();
                    String columnName = pushdownInputColumn.getName();
                    if (!columnName.equalsIgnoreCase(column)) {
                        // Pinot can only handle the direct column input to `dateTimeConvert` function
                        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unsupported expr: " + function);
                    }
                }
                else if (timeConversion.getInputs().get(0) instanceof PushDownCastExpression) {
                    PushDownCastExpression pushdownInputColumn = (PushDownCastExpression) timeConversion.getInputs().get(0);
                    column = pushdownInputColumn.accept(this, context).getDefinition();
                }
                else {
                    throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                            "input to time conversion function is not supported type: " + timeConversion.getInputs().get(0));
                }

                inputColumn = column;
                inputFormat = "'1:SECONDS:EPOCH'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getName());
        }

        String outputFormat = "'1:MILLISECONDS:EPOCH'";
        String outputGranularity;

        PushDownExpression intervalParameter = function.getInputs().get(0);
        if (!(intervalParameter instanceof PushDownLiteral)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        PushDownLiteral intervalUnit = (PushDownLiteral) intervalParameter;
        switch (intervalUnit.getStrValue()) {
            case "second":
                outputGranularity = "'1:SECONDS'";
                break;
            case "minute":
                outputGranularity = "'1:MINUTES'";
                break;
            case "hour":
                outputGranularity = "'1:HOURS'";
                break;
            case "day":
                outputGranularity = "'1:DAYS'";
                break;
            case "week":
                outputGranularity = "'1:WEEKS'";
                break;
            case "month":
                outputGranularity = "'1:MONTHS'";
                break;
            case "quarter":
                outputGranularity = "'1:QUARTERS'";
                break;
            case "year":
                outputGranularity = "'1:YEARS'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                        "interval in date_trunc is not supported: " + intervalUnit.getStrValue());
        }

        return derived("dateTimeConvert(" + inputColumn + ", " + inputFormat + ", " + outputFormat + ", " + outputGranularity + ")");
    }

    @Override
    public PinotExpression visitLogicalBinary(PushDownLogicalBinaryExpression logical, Map<String, Selection> context)
    {
        return derived(format("(%s %s %s)",
                logical.getLeft().accept(this, context).definition, logical.getOperator(), logical.getRight().accept(this, context).definition));
    }

    @Override
    public PinotExpression visitInExpression(PushDownInExpression in, Map<String, Selection> context)
    {
        return derived(format("(%s IN (%s))",
                in.getValue().accept(this, context).definition,
                in.getArguments().stream()
                        .map(a -> a.accept(this, context).definition)
                        .collect(Collectors.joining(", "))));
    }

    @Override
    public PinotExpression visitBetweenExpression(PushDownBetweenExpression between, Map<String, Selection> context)
    {
        return derived(format("(%s BETWEEN %s AND %s)",
                between.getValue().accept(this, context).definition,
                between.getLeft().accept(this, context).definition,
                between.getRight().accept(this, context).definition));
    }

    @Override
    public PinotExpression visitNotExpression(PushDownNotExpression not, Map<String, Selection> context)
    {
        return derived(format("NOT %s", not.getInput().accept(this, context).definition));
    }

    @Override
    public PinotExpression visitLiteral(PushDownLiteral literal, Map<String, Selection> context)
    {
        return new PinotExpression(literal.toString(), Origin.LITERAL);
    }

    @Override
    public PinotExpression visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        if (!(cast.getInput() instanceof PushDownInputColumn)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Not supported: " + cast);
        }
        PushDownInputColumn inputColumn = (PushDownInputColumn) cast.getInput();
        // Get the type input column
        Type inputType = context.get(inputColumn.getName()).getDataType();

        if (inputColumn == null) {
            throw new IllegalArgumentException("column doesn't exists in input");
        }

        boolean isImplicitCast;
        Class<?> inputJavaType = inputType.getJavaType();
        switch (cast.getType().toLowerCase(ENGLISH)) {
            case "double":
                isImplicitCast = inputJavaType.equals(double.class) || inputJavaType.equals(long.class);
                break;

            case "bigint":
                isImplicitCast = inputJavaType.equals(long.class);
                break;

            case "varchar":
                isImplicitCast = inputJavaType.equals(Slice.class);
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "unsupported cast: " + cast);
        }

        if (isImplicitCast) {
            return inputColumn.accept(this, context);
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Only implicit cast is supported: " + cast);
    }

    @Override
    public PinotExpression visitArithmeticExpression(PushDownArithmeticExpression expression, Map<String, Selection> context)
    {
        PinotExpression right = expression.getRight().accept(this, context);
        if (expression.getLeft() == null) {
            // unary ...
            String prefix = expression.getOperator().equals("-") ? "-" : "";
            return derived(prefix + right.definition);
        }

        PinotExpression left = expression.getLeft().accept(this, context);
        String prestoOp = expression.getOperator();
        String pinotOp = PRESTO_TO_PINOT_OP.get(prestoOp);
        if (pinotOp == null) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported binary expression " + prestoOp);
        }

        return derived(format("%s(%s, %s)", pinotOp, left.definition, right.definition));
    }

    public static class PinotExpression
    {
        private final String definition;
        private final Origin origin;

        PinotExpression(String definition, Origin origin)
        {
            this.definition = definition;
            this.origin = origin;
        }

        static PinotExpression derived(String definition)
        {
            return new PinotExpression(definition, Origin.DERIVED);
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }
    }
}

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

import com.facebook.presto.aresdb.query.AresDbQueryGeneratorContext.Selection;
import com.facebook.presto.spi.pipeline.PushDownBetweenExpression;
import com.facebook.presto.spi.pipeline.PushDownCastExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownExpressionVisitor;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;

import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AresDbTimeFilterExtractor
        extends PushDownExpressionVisitor<AresDbTimeFilterExtractor.AresDbFilter, Map<String, Selection>>
{
    private final String timeColumn;

    private AresDbTimeFilterExtractor(String timeColumn)
    {
        this.timeColumn = timeColumn;
    }

    public static AresDbFilter extract(PushDownExpression predicate, String timeColumn, Map<String, Selection> context)
    {
        return predicate.accept(new AresDbTimeFilterExtractor(timeColumn), context);
    }

    private static PushDownExpression combine(Optional<PushDownExpression> filter1, PushDownExpression filter2)
    {
        if (filter1.isPresent()) {
            return new PushDownLogicalBinaryExpression(filter1.get(), "AND", filter2);
        }

        return filter2;
    }

    @Override
    public AresDbFilter visitExpression(PushDownExpression expression, Map<String, Selection> context)
    {
        return null;
    }

    @Override
    public AresDbFilter visitLogicalBinary(PushDownLogicalBinaryExpression comparision, Map<String, Selection> context)
    {
        AresDbFilter left = comparision.getLeft().accept(this, context);
        AresDbFilter right = comparision.getRight().accept(this, context);

        switch (comparision.getOperator().toUpperCase(ENGLISH)) {
            case "AND": {
                if (left.containsTimeFilter()) {
                    return new AresDbFilter(left.timeFilter, Optional.of(combine(left.getNonTimeFilter(), comparision.getRight())));
                }

                if (right.containsTimeFilter()) {
                    return new AresDbFilter(right.timeFilter, Optional.of(combine(right.getNonTimeFilter(), comparision.getLeft())));
                }
            }
            // fallback to default
            default:
                return new AresDbFilter(Optional.empty(), Optional.of(comparision));
        }
    }

    @Override
    public AresDbFilter visitLiteral(PushDownLiteral literal, Map<String, Selection> context)
    {
        return new AresDbFilter(Optional.empty(), Optional.of(literal));
    }

    @Override
    public AresDbFilter visitBetweenExpression(PushDownBetweenExpression between, Map<String, Selection> context)
    {
        if (isTimeColumn(between.getValue(), context)) {
            if (isLiteral(between.getRight()) && isLiteral(between.getLeft())) {
                return new AresDbFilter(Optional.of(between), Optional.empty());
            }
        }

        return new AresDbFilter(Optional.empty(), Optional.of(between));
    }

    private boolean isTimeColumn(PushDownExpression expression, Map<String, Selection> context)
    {
        if (!(expression instanceof PushDownInputColumn)) {
            return false;
        }

        PushDownInputColumn inputColumn = (PushDownInputColumn) expression;

        Selection input = requireNonNull(context.get(inputColumn.getName()), format("Input column %s does not exist in the input", inputColumn.getName()));
        return input.getDefinition().equalsIgnoreCase(timeColumn);
    }

    private boolean isLiteral(PushDownExpression expression)
    {
        return expression instanceof PushDownLiteral;
    }

    @Override
    public AresDbFilter visitCastExpression(PushDownCastExpression cast, Map<String, Selection> context)
    {
        return new AresDbFilter(Optional.empty(), Optional.of(cast.getInput()));
    }

    public static class AresDbFilter
    {
        private final Optional<PushDownExpression> timeFilter;
        private final Optional<PushDownExpression> nonTimeFilter;

        private AresDbFilter(Optional<PushDownExpression> timeFilter, Optional<PushDownExpression> nonTimeFilter)
        {
            this.timeFilter = timeFilter;
            this.nonTimeFilter = nonTimeFilter;
        }

        public boolean containsTimeFilter()
        {
            return timeFilter.isPresent();
        }

        public Optional<PushDownExpression> getTimeFilter()
        {
            return timeFilter;
        }

        public Optional<PushDownExpression> getNonTimeFilter()
        {
            return nonTimeFilter;
        }
    }
}

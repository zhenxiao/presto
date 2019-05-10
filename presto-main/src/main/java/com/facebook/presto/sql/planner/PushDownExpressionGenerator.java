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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.pipeline.PushDownArithmeticExpression;
import com.facebook.presto.spi.pipeline.PushDownBetweenExpression;
import com.facebook.presto.spi.pipeline.PushDownCastExpression;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.facebook.presto.spi.pipeline.PushDownFunction;
import com.facebook.presto.spi.pipeline.PushDownInExpression;
import com.facebook.presto.spi.pipeline.PushDownInputColumn;
import com.facebook.presto.spi.pipeline.PushDownLiteral;
import com.facebook.presto.spi.pipeline.PushDownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.PushDownNotExpression;
import com.facebook.presto.sql.planner.iterative.rule.PushDownUtils;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PushDownExpressionGenerator
        extends AstVisitor<PushDownExpression, Void>
{
    private final PushDownUtils.ExpressionToTypeConverter expressionToTypeConverter;

    public PushDownExpressionGenerator(PushDownUtils.ExpressionToTypeConverter expressionToTypeConverter)
    {
        this.expressionToTypeConverter = expressionToTypeConverter;
    }

    @Override
    protected PushDownExpression visitFunctionCall(FunctionCall node, Void context)
    {
        List<Expression> inputs = node.getArguments();
        List<PushDownExpression> pushdownInputs = new ArrayList<>();

        for (Expression input : inputs) {
            PushDownExpression pushdownExpression = this.process(input);
            if (pushdownExpression == null) {
                return null;
            }
            pushdownInputs.add(pushdownExpression);
        }

        return new PushDownFunction(expressionToTypeConverter.getTypeSignature(node), node.getName().toString(), pushdownInputs);
    }

    @Override
    protected PushDownExpression visitComparisonExpression(ComparisonExpression node, Void context)
    {
        PushDownExpression left = this.process(node.getLeft());
        PushDownExpression right = this.process(node.getRight());
        String operator = node.getOperator().getValue();

        if (left == null || right == null) {
            return null;
        }

        return new PushDownLogicalBinaryExpression(expressionToTypeConverter.getTypeSignature(node), left, operator, right);
    }

    @Override
    protected PushDownExpression visitBetweenPredicate(BetweenPredicate node, Void context)
    {
        PushDownExpression left = this.process(node.getMin());
        PushDownExpression right = this.process(node.getMax());
        PushDownExpression value = this.process(node.getValue());

        if (left == null || right == null || value == null) {
            return null;
        }

        return new PushDownBetweenExpression(expressionToTypeConverter.getTypeSignature(node), value, left, right);
    }

    @Override
    protected PushDownExpression visitInPredicate(InPredicate node, Void context)
    {
        List<PushDownExpression> arguments = new ArrayList<>();
        if (!(node.getValueList() instanceof InListExpression)) {
            return null;
        }

        InListExpression inList = (InListExpression) node.getValueList();
        for (Expression inValue : inList.getValues()) {
            PushDownExpression out = this.process(inValue);
            if (out == null) {
                return null;
            }

            arguments.add(out);
        }

        PushDownExpression value = this.process(node.getValue());
        if (value == null) {
            return null;
        }

        return new PushDownInExpression(expressionToTypeConverter.getTypeSignature(node), true, value, arguments);
    }

    @Override
    protected PushDownExpression visitNotExpression(NotExpression node, Void context)
    {
        PushDownExpression input = this.process(node.getValue());
        if (input == null) {
            return null;
        }

        return new PushDownNotExpression(expressionToTypeConverter.getTypeSignature(node), input);
    }

    @Override
    protected PushDownExpression visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        return new PushDownLiteral(expressionToTypeConverter.getTypeSignature(node), null, null, node.getValue(), null);
    }

    @Override
    protected PushDownExpression visitLongLiteral(LongLiteral node, Void context)
    {
        return new PushDownLiteral(expressionToTypeConverter.getTypeSignature(node), null, node.getValue(), null, null);
    }

    @Override
    protected PushDownExpression visitStringLiteral(StringLiteral node, Void context)
    {
        return new PushDownLiteral(expressionToTypeConverter.getTypeSignature(node), node.getValue(), null, null, null);
    }

    @Override
    protected PushDownExpression visitGenericLiteral(GenericLiteral node, Void context)
    {
        // Opposite of LiteralEncoder that creates these GenericLiteral's
        if ("BIGINT".equalsIgnoreCase(node.getType())) {
            return visitLongLiteral(new LongLiteral(node.getValue()), null);
        }
        if ("SMALLINT".equalsIgnoreCase(node.getType())) {
            return visitLongLiteral(new LongLiteral(node.getValue()), null);
        }
        if ("TINYINT".equalsIgnoreCase(node.getType())) {
            return visitLongLiteral(new LongLiteral(node.getValue()), null);
        }
        if ("REAL".equalsIgnoreCase(node.getType())) {
            return visitDoubleLiteral(new DoubleLiteral(node.getValue()), null);
        }
        return null;
    }

    @Override
    protected PushDownExpression visitBooleanLiteral(BooleanLiteral node, Void context)
    {
        return new PushDownLiteral(expressionToTypeConverter.getTypeSignature(node), null, null, null, node.getValue());
    }

    @Override
    protected PushDownExpression visitCharLiteral(CharLiteral node, Void context)
    {
        return new PushDownLiteral(expressionToTypeConverter.getTypeSignature(node), node.getValue(), null, null, null);
    }

    @Override
    protected PushDownExpression visitSymbolReference(SymbolReference node, Void context)
    {
        return new PushDownInputColumn(expressionToTypeConverter.getTypeSignature(node), node.getName());
    }

    @Override
    protected PushDownExpression visitCast(Cast node, Void context)
    {
        // Handle cast where the input is already in required type
        Expression input = node.getExpression();
        String resultType = node.getType();

        PushDownExpression newInput = this.process(input);
        PushDownCastExpression cast;
        if (newInput != null) {
            cast = new PushDownCastExpression(expressionToTypeConverter.getTypeSignature(node), newInput, resultType, expressionToTypeConverter.canCoerce(expressionToTypeConverter.getType(input), expressionToTypeConverter.getType(node)));
        }
        else {
            return null;
        }

        if (cast.isImplicitCast(Optional.empty())) {
            return cast.getInput();
        }
        else {
            return cast;
        }
    }

    @Override
    protected PushDownExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        PushDownExpression left = this.process(node.getLeft());
        PushDownExpression right = this.process(node.getRight());
        String operator = node.getOperator().toString();

        if (left == null || right == null) {
            return null;
        }

        return new PushDownLogicalBinaryExpression(expressionToTypeConverter.getTypeSignature(node), left, operator, right);
    }

    @Override
    protected PushDownExpression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        PushDownExpression left = this.process(node.getLeft());
        PushDownExpression right = this.process(node.getRight());
        String operator = node.getOperator().getValue();

        if (left == null || right == null) {
            return null;
        }

        return new PushDownArithmeticExpression(expressionToTypeConverter.getTypeSignature(node), left, operator, right);
    }

    @Override
    protected PushDownExpression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
    {
        PushDownExpression right = this.process(node.getValue());
        if (right == null) {
            return null;
        }
        return new PushDownArithmeticExpression(expressionToTypeConverter.getTypeSignature(node), null, node.getSign() == ArithmeticUnaryExpression.Sign.MINUS ? "-" : "+", right);
    }
}

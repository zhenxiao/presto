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
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Objects;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPushDownExpressionGenerator
{
    private static void test(String inputSql, PushDownExpression expected)
    {
        Expression expression = expression(inputSql);
        PushDownExpression actual = new PushDownExpressionGenerator((expr) -> BIGINT).process(expression);
        assertEquals(actual.toString(), expected.toString());
    }

    private static PushDownExpression pdInputColumn(String column)
    {
        return new PushDownInputColumn(BIGINT.getTypeSignature(), column);
    }

    private static PushDownExpression pdLiteral(Long value)
    {
        return new PushDownLiteral(BIGINT.getTypeSignature(), null, value, null, null);
    }

    private static PushDownExpression pdLiteral(String value)
    {
        return new PushDownLiteral(VARCHAR.getTypeSignature(), value, null, null, null);
    }

    private static PushDownExpression pdFunction(String name, PushDownExpression... inputs)
    {
        return new PushDownFunction(BIGINT.getTypeSignature(), name, Arrays.asList(inputs));
    }

    private static PushDownExpression pdBetween(PushDownExpression value, PushDownExpression l, PushDownExpression r)
    {
        return new PushDownBetweenExpression(BOOLEAN.getTypeSignature(), value, l, r);
    }

    private static PushDownExpression pdIn(boolean whiteList, PushDownExpression value, PushDownExpression... inList)
    {
        return new PushDownInExpression(BOOLEAN.getTypeSignature(), whiteList, value, Arrays.asList(inList));
    }

    private static PushDownExpression pdNot(PushDownExpression input)
    {
        return new PushDownNotExpression(BOOLEAN.getTypeSignature(), input);
    }

    private static PushDownExpression pdLogicalBinary(PushDownExpression left, String op, PushDownExpression right)
    {
        return new PushDownLogicalBinaryExpression(BOOLEAN.getTypeSignature(), left, op, right);
    }

    private static PushDownExpression pdCast(PushDownExpression input, TypeSignature type)
    {
        return new PushDownCastExpression(type, input, type.getBase(), Objects.equals(input.getType(), type));
    }

    private static PushDownExpression pdArith(PushDownExpression left, String op, PushDownExpression right)
    {
        return new PushDownArithmeticExpression((left == null ? right : left).getType(), left, op, right);
    }

    @Test
    public void testAll()
    {
        // functions
        test("fun(a, b)", pdFunction("fun", pdInputColumn("a"), pdInputColumn("b")));
        test("fun(1, b)", pdFunction("fun", pdLiteral(1L), pdInputColumn("b")));

        // IN list
        test("col in (1, 2, 3)", pdIn(true, pdInputColumn("col"), pdLiteral(1L), pdLiteral(2L), pdLiteral(3L)));
        test("col in ('val1', 'val2', 'val3')", pdIn(true, pdInputColumn("col"), pdLiteral("val1"), pdLiteral("val2"), pdLiteral("val3")));

        // NOT IN
        test("col not in (1, 2, 3)", pdNot(pdIn(true, pdInputColumn("col"), pdLiteral(1L), pdLiteral(2L), pdLiteral(3L))));
        test("col not in ('val1', 'val2', 'val3')", pdNot(pdIn(true, pdInputColumn("col"), pdLiteral("val1"), pdLiteral("val2"), pdLiteral("val3"))));

        // logical binary
        test("a and b", pdLogicalBinary(pdInputColumn("a"), "AND", pdInputColumn("b")));
        test("fun(a) and fun(b)", pdLogicalBinary(pdFunction("fun", pdInputColumn("a")), "AND", pdFunction("fun", pdInputColumn("b"))));

        // between
        test("a between 234 and 235", pdBetween(pdInputColumn("a"), pdLiteral(234L), pdLiteral(235L)));

        // arithmetic (two operands)
        for (String op : ImmutableList.of("+", "-", "*", "/")) {
            test(format("a %s b", op), pdArith(pdInputColumn("a"), op, pdInputColumn("b")));
            test(format("a %s 1", op), pdArith(pdInputColumn("a"), op, pdLiteral(1L)));
            test(format("2 %s b", op), pdArith(pdLiteral(2L), op, pdInputColumn("b")));
            test(format("2 %s 4", op), pdArith(pdLiteral(2L), op, pdLiteral(4L)));
        }

        // arithmetic unary operator
        test("-1", pdArith(null, "-", pdLiteral(1L)));

        // combination
        test("fun(a) and fun(b) or log(1) = 23",
                pdLogicalBinary(
                        pdLogicalBinary(pdFunction("fun", pdInputColumn("a")), "AND", pdFunction("fun", pdInputColumn("b"))),
                        "OR",
                        pdLogicalBinary(pdFunction("log", pdLiteral(1L)), "=", pdLiteral(23L))));

        test("date_trunc('hour', cast(from_unixtime(secondssinceepoch) as timestamp))",
                pdFunction("date_trunc",
                        pdLiteral("hour"),
                        pdCast(pdFunction(
                                "from_unixtime",
                                pdInputColumn("secondssinceepoch")),
                                TimestampType.TIMESTAMP.getTypeSignature())));
    }
}

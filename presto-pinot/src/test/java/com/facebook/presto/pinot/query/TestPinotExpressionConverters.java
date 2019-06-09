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
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.PinotTestUtils.pdExpr;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPinotExpressionConverters
{
    private static final Map<String, Selection> TEST_INPUT = ImmutableMap.of(
            "regionid", new Selection("regionId", TABLE_COLUMN, BIGINT), // direct column reference
            "city", new Selection("city", TABLE_COLUMN, VARCHAR), // direct column reference
            "totalfare", new Selection("(fare + trip)", DERIVED, DOUBLE), // derived column
            "secondssinceepoch", new Selection("secondsSinceEpoch", TABLE_COLUMN, BIGINT)); // column for datetime functions

    @Test
    public void testProjectExpressionConverter()
    {
        // functions
        testProject("date_trunc('hour', from_unixtime(secondssinceepoch))",
                "dateTimeConvert(secondsSinceEpoch, '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')");

        // arithmetic
        testProject("regionid + 1", "ADD(regionId, 1)");
        testProject("regionid - 1", "SUB(regionId, 1)");
        testProject("1 * regionid", "MULT(1, regionId)");
        testProject("1 / regionid", "DIV(1, regionId)");
        testProject("-1", "-1");
        testProject("-regionid", "-regionId");

        // combinations
        testProject("date_trunc('hour', from_unixtime(secondssinceepoch + 2))",
                "dateTimeConvert(ADD(secondsSinceEpoch, 2), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')");

        testProject("secondssinceepoch + 1559978258.674", "ADD(secondsSinceEpoch, 1559978258.674000)");
        testProject("secondssinceepoch + 1559978258", "ADD(secondsSinceEpoch, 1559978258)");
    }

    @Test
    public void testFilterExpressionConverter()
    {
        // Simple comparisons
        testFilter("regionid = 20", "(regionId = 20)");
        testFilter("regionid >= 20", "(regionId >= 20)");
        testFilter("city = 'Campbell'", "(city = 'Campbell')");

        // between
        testFilter("totalfare between 20 and 30", "((fare + trip) BETWEEN 20 AND 30)");

        // in, not in
        testFilter("regionid in (20, 30, 40)", "(regionId IN (20, 30, 40))");
        testFilter("regionid not in (20, 30, 40)", "(regionId NOT IN (20, 30, 40))");
        testFilter("city in ('San Jose', 'Campbell', 'Union City')", "(city IN ('San Jose', 'Campbell', 'Union City'))");
        testFilter("city not in ('San Jose', 'Campbell', 'Union City')", "(city NOT IN ('San Jose', 'Campbell', 'Union City'))");
        testFilterUnsupported("secondssinceepoch + 1 in (234, 24324)");
        testFilterUnsupported("NOT (secondssinceepoch = 2323)");

        // combinations
        testFilter("totalfare between 20 and 30 AND regionid > 20 OR city = 'Campbell'",
                "((((fare + trip) BETWEEN 20 AND 30) AND (regionId > 20)) OR (city = 'Campbell'))");

        testFilter("secondssinceepoch > 1559978258", "(secondsSinceEpoch > 1559978258)");
    }

    private void testProject(String sqlExpression, String expectedPinotExpression)
    {
        PushDownExpression pushDownExpression = pdExpr(sqlExpression);
        String actualPinotExpression = pushDownExpression.accept(new PinotProjectExpressionConverter(), TEST_INPUT).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testFilter(String sqlExpression, String expectedPinotExpression)
    {
        PushDownExpression pushDownExpression = pdExpr(sqlExpression);
        String actualPinotExpression = pushDownExpression.accept(new PinotFilterExpressionConverter(), TEST_INPUT).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testFilterUnsupported(String sqlExpression)
    {
        try {
            PushDownExpression pushDownExpression = pdExpr(sqlExpression);
            pushDownExpression.accept(new PinotFilterExpressionConverter(), TEST_INPUT).getDefinition();
            fail("expected to not reach here");
        }
        catch (PinotException e) {
            assertEquals(e.getErrorCode(), PINOT_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }
}

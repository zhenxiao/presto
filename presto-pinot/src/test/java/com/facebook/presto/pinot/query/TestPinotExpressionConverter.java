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

import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.pipeline.PushDownExpression;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.pinot.PinotTestUtils.pdExpr;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.DERIVED;
import static com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin.TABLE_COLUMN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestPinotExpressionConverter
{
    private static final Map<String, Selection> TEST_INPUT = ImmutableMap.of(
            "regionid", new Selection("regionId", TABLE_COLUMN, BIGINT), // direct column reference
            "city", new Selection("city", TABLE_COLUMN, VARCHAR), // direct column reference
            "totalfare", new Selection("(fare + trip)", DERIVED, DOUBLE), // derived column
            "secondssinceepoch", new Selection("secondsSinceEpoch", TABLE_COLUMN, BIGINT)); // column for datetime functions

    @Test
    public void testAll()
    {
        // Simple comparisons
        test("regionid = 20", "(regionId = 20)");
        test("regionid >= 20", "(regionId >= 20)");
        test("city = 'Campbell'", "(city = 'Campbell')");

        // between
        test("totalfare between 20 and 30", "((fare + trip) BETWEEN 20 AND 30)");

        // in, not in
        test("regionid in (20, 30, 40)", "(regionId IN (20, 30, 40))");
        test("regionid not in (20, 30, 40)", "NOT (regionId IN (20, 30, 40))");
        test("city in ('San Jose', 'Campbell', 'Union City')", "(city IN ('San Jose', 'Campbell', 'Union City'))");
        test("city not in ('San Jose', 'Campbell', 'Union City')", "NOT (city IN ('San Jose', 'Campbell', 'Union City'))");

        // functions
        test("date_trunc('hour', from_unixtime(secondssinceepoch))",
                "dateTimeConvert(secondsSinceEpoch, '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')");

        // arithmetic
        test("regionid + 1", "ADD(regionId, 1)");
        test("regionid - 1", "SUB(regionId, 1)");
        test("1 * regionid", "MULT(1, regionId)");
        test("1 / regionid", "DIV(1, regionId)");
        test("-1", "-1");
        test("-regionid", "-regionId");

        // combinations
        test("date_trunc('hour', from_unixtime(secondssinceepoch)) and regionid not in (20, 30, 40)",
                "(dateTimeConvert(secondsSinceEpoch, '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS') AND " +
                        "NOT (regionId IN (20, 30, 40)))");

        test("totalfare between 20 and 30 AND regionid > 20 OR city = 'Campbell'",
                "((((fare + trip) BETWEEN 20 AND 30) AND (regionId > 20)) OR (city = 'Campbell'))");
    }

    private void test(String sqlExpression, String expectedPinotExpression)
    {
        PushDownExpression pushDownExpression = pdExpr(sqlExpression);
        String actualPinotExpression = pushDownExpression.accept(new PinotExpressionConverter(), TEST_INPUT).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }
}

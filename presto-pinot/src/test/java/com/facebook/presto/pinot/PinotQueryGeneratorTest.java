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
package com.facebook.presto.pinot;

import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotQueryGenerator.getPinotQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PinotQueryGeneratorTest
{
    final List<PinotColumnHandle> columnHandlesNoAgg = new ArrayList<>();
    final List<PinotColumnHandle> columnHandlesWithAgg = new ArrayList<>();

    @BeforeTest
    void init()
    {
        columnHandlesNoAgg.add(new PinotColumnHandle("pinot", "varchar", VARCHAR, 0, Optional.empty()));
        columnHandlesNoAgg.add(new PinotColumnHandle("pinot", "int", INTEGER, 1, Optional.empty()));
        columnHandlesNoAgg.add(new PinotColumnHandle("pinot", "secondsSinceEpoch", BIGINT, 2, Optional.empty()));
        columnHandlesNoAgg.add(new PinotColumnHandle("pinot", "boolean", BOOLEAN, 3, Optional.empty()));
        columnHandlesNoAgg.add(new PinotColumnHandle("pinot", "double", DOUBLE, 4, Optional.empty()));

        columnHandlesWithAgg.add(new PinotColumnHandle("pinot", "varchar", VARCHAR, 0, Optional.of("count")));
        columnHandlesWithAgg.add(new PinotColumnHandle("pinot", "int", INTEGER, 1, Optional.of("max")));
        columnHandlesWithAgg.add(new PinotColumnHandle("pinot", "boolean", INTEGER, 1, Optional.empty()));
    }

    @Test
    public void testGetPinotQuerySelectAll()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table  LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesNoAgg, false, "", "", "table", 10));
    }

    @Test
    public void testGetPinotQueryWithPredicate()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesNoAgg, false, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", 10));
    }

    @Test
    public void testGetPinotQueryWithAggregationDisabled()
    {
        String expectedQuery = "SELECT varchar, int, boolean FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesWithAgg, false, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", 10));
    }

    @Test
    public void testGetPinotQueryWithAggregation()
    {
        String expectedQuery = "SELECT count(varchar), max(int) FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesWithAgg, true, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", 10));
    }
}

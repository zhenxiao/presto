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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PinotPageSourceTest
{
    PinotPageSource pinotPageSource;
    final List<PinotColumnHandle> columnHandles = new ArrayList<>();
    Map<String, List<String>> aggregations = new HashMap<>();
    PinotSplit split;

    @BeforeTest
    void init()
    {
        columnHandles.add(new PinotColumnHandle("pinot", "varchar", VARCHAR, 0, Optional.empty()));
        columnHandles.add(new PinotColumnHandle("pinot", "int", INTEGER, 1, Optional.empty()));
        columnHandles.add(new PinotColumnHandle("pinot", "secondsSinceEpoch", BIGINT, 2, Optional.empty()));
        columnHandles.add(new PinotColumnHandle("pinot", "boolean", BOOLEAN, 3, Optional.empty()));
        columnHandles.add(new PinotColumnHandle("pinot", "double", DOUBLE, 4, Optional.empty()));
        split = new PinotSplit(
                "pinot", "table", "host", "segment", new PinotColumn("secondsSinceEpoch", INTEGER),
                "secondsSinceEpoch > 10000",
                "int > 3",
                10,
                aggregations);
    }

    @Test
    void testShouldPushAggregation()
    {
        aggregations.put("secondsSinceEpoch", Arrays.asList("max"));
        pinotPageSource = new PinotPageSource(new PinotConfig(), new PinotScatterGatherQueryClient(new PinotConfig()), split, columnHandles);
        Assert.assertTrue(pinotPageSource.shouldPushAggregation(aggregations));
        aggregations.put("secondsSinceEpoch", Arrays.asList("max", "min"));
        Assert.assertFalse(pinotPageSource.shouldPushAggregation(aggregations));
    }
}

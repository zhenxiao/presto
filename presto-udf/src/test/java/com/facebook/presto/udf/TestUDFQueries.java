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
package com.facebook.presto.udf;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestUDFQueries
        extends AbstractTestQueryFramework
{
    public TestUDFQueries()
    {
        super(TestUDFQueries::createLocalQueryRunner);
    }

    @Test
    public void testDistance()
            throws Exception
    {
        assertQuery("select distance(45.0, 0.0, 45.0, 1.0)", "select 78.6262959272162");
        assertQuery("select floor(distance(90.0, -180.0, 90.0, 180.0))", "select 0.0");
    }

    @Test
    public void testVincentyDistance()
            throws Exception
    {
        assertQuery("select vincenty_distance(45.0, 0.0, 45.0, 1.0)", "select 78.84633470959047");
    }

    @Test
    public void testWithinCircle()
            throws Exception
    {
        assertQuery("select within_circle(13.6,43.5,48.5,45.5,3880)", "select FALSE");
        assertQuery("select within_circle(13.6,43.5,48.5,45.5,3890)", "select TRUE");
    }

    @Test
    public void testInitCap()
            throws Exception
    {
        assertQuery("select initcap('tHe soap')", "select 'The Soap'");
        assertQuery("select initcap(NULL)", "select NULL");
    }

    @Test
    public void testRegexpSubstr()
            throws Exception
    {
        assertQuery("SELECT REGEXP_SUBSTR('healthy, wealthy, and wise','\\w+thy')", "SELECT 'healthy'");
        assertQuery("SELECT REGEXP_SUBSTR('healthy, wealthy, and wise','\\w+thy',2)", "select 'ealthy'");
        assertQuery("SELECT REGEXP_SUBSTR('healthy, wealthy, and wise','\\w+thy',1,2)", "select 'wealthy'");
        assertQuery("SELECT REGEXP_SUBSTR('one two three', '(\\w+)\\s+(\\w+)\\s+(\\w+)', 1, 1, '', 3)", "select 'three'");
    }

    @Test
    public void testRegexpCount()
            throws Exception
    {
        assertQuery("SELECT REGEXP_COUNT('a man, a plan, a canal: Panama', 'an')", "select 4");
        assertQuery("SELECT REGEXP_COUNT('a man, a plan, a canal: Panama', 'an', 5)", "select 3");
        assertQuery("SELECT REGEXP_COUNT('a man, a plan, a canal: Panama', '[a-z]an')", "select 3");
        assertQuery("SELECT REGEXP_COUNT('a man, a plan, a canal: Panama', '[a-z]an', 1, 'i')", "select 4");
    }

    @Test
    public void testInStr()
            throws Exception
    {
        // Test results should be the same with instr
        assertQuery("SELECT indexOf('abc', '')", "select 1");
        assertQuery("SELECT indexOf('', 'b')", "select 0");
        assertQuery("SELECT indexOf('abc', 'b')", "select 2");
        assertQuery("SELECT indexOf('aébc', 'b')", "select 3");
    }

    @Test
    public void testDays()
            throws Exception
    {
        assertQuery("SELECT DAYS (DATE '2011-01-22')", "select 734159");
        assertQuery("SELECT DAYS (DATE '1999-12-31')", "select 730119");
    }

    @Test
    public void testTimestampRound()
            throws Exception
    {
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:34:40.800', 'SS')", "SELECT TIMESTAMP '2011-09-14 12:34:41'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:34:40', 'mi')", "SELECT TIMESTAMP '2011-09-14 12:35:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:34:00', 'hh12')", "SELECT TIMESTAMP '2011-09-14 13:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:34:00', 'ddd')", "SELECT TIMESTAMP '2011-09-15 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:34:00', 'mon')", "SELECT TIMESTAMP '2011-09-01 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-24 12:34:00', 'mon')", "SELECT TIMESTAMP '2011-10-01 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-04-22 12:34:00', 'year')", "SELECT TIMESTAMP '2011-01-01 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-22 12:34:00', 'year')", "SELECT TIMESTAMP '2012-01-01 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-22 12:34:00', 'day')", "SELECT TIMESTAMP '2011-09-18 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-24 12:34:00', 'day')", "SELECT TIMESTAMP '2011-09-25 00:00:00'");
        assertQuery("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-24 12:34:00')", "SELECT TIMESTAMP '2011-09-25 00:00:00'");
        assertQueryFails("SELECT TIMESTAMP_ROUND(TIMESTAMP '2011-09-14 12:00:00', 'bad_precision')", "bad_precision is not a valid precision for function TIMESTAMP_ROUND");
    }

    @Test
    public void testTimestampToTimezone()
            throws Exception
    {
        assertQuery("SELECT to_timestamp_tz(TIMESTAMP '2012-02-24 13:00:00', 'GMT', 'PST')", "SELECT TIMESTAMP '2012-02-24 05:00:00.000 America/Los_Angeles'");
    }

    @Test
    public void testUberAsGeoJSON()
            throws Exception
    {
        assertQuery("select uber_asgeojson(1,2)", "select '{\"type\":\"Point\",\"coordinates\":[1.0,2.0]}'");
    }

    @Test
    public void testTranslate()
            throws Exception
    {
        assertQuery("SELECT TRANSLATE(NULL, 'bc', 'yz')", "select NULL");
        assertQuery("SELECT TRANSLATE('abc', NULL, 'yz')", "select NULL");
        assertQuery("SELECT TRANSLATE('abc', 'bc', NULL)", "select NULL");
        assertQuery("SELECT TRANSLATE('abc', 'b', '')", "select 'ac'");
        assertQuery("SELECT TRANSLATE('abcba', 'bc', 'yz')", "select 'ayzya'");
        assertQuery("SELECT TRANSLATE('straße', 'ß', 'ss')", "select 'strase'");
    }
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setTimeZoneKey(TIME_ZONE_KEY)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        UserDefinedFunctionsPlugin plugin = new UserDefinedFunctionsPlugin();
        localQueryRunner.getMetadata().addFunctions(extractFunctions(new UserDefinedFunctionsPlugin().getFunctions()));

        return localQueryRunner;
    }
}

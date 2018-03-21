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

/**
 * Test class that tests the Presto UDFs that are compatible to Vertica Date/Time/Timestamp functions
 */
public class TestVerticaDateTimeQueries
        extends AbstractTestQueryFramework
{
    protected TestVerticaDateTimeQueries()
    {
        super(TestVerticaDateTimeQueries::createLocalQueryRunner);
    }

    @Test
    public void testAddMonths()
            throws Exception
    {
        assertQuery("select ADD_MONTHS(DATE '2008-01-31', 3)", "select DATE '2008-04-30'");
        assertQuery("select ADD_MONTHS(TIMESTAMP '2008-01-29 23:30:05', 1)", "select TIMESTAMP '2008-02-29 23:30:05'");
        assertQuery("select ADD_MONTHS(TIMESTAMP '2008-01-29 23:30:05 Europe/Berlin', 1)", "select TIMESTAMP '2008-02-29 23:30:05 Europe/Berlin'");
    }

    @Test
    public void testAgeInMonths()
            throws Exception
    {
        assertQuery("select AGE_IN_MONTHS(DATE '2008-01-23', DATE '2008-11-25')", "select 10");
        assertQuery("select AGE_IN_MONTHS(TIMESTAMP '2008-01-29 23:30:05', TIMESTAMP '2010-03-27')", "select 25");
        assertQuery("select AGE_IN_MONTHS(TIMESTAMP '2018-01-15 23:30:05 Asia/Singapore', TIMESTAMP '2018-03-14 23:30:05 America/Los_Angeles')", "select 1");
    }

    @Test
    public void testAgeInYears()
            throws Exception
    {
        assertQuery("select AGE_IN_YEARS(DATE '2008-01-23', DATE '2012-11-25')", "select 4");
        assertQuery("select AGE_IN_YEARS(TIMESTAMP '2008-04-29 23:30:05', TIMESTAMP '2010-03-27')", "select 1");
        assertQuery("select AGE_IN_YEARS(TIMESTAMP '2018-01-15 23:30:05 Asia/Singapore', TIMESTAMP '2018-03-14 23:30:05 America/Los_Angeles')", "select 0");
    }

    @Test
    public void testDatePart()
            throws Exception
    {
        assertQuery("select DATE_PART('DAY', DATE '2001-02-16')", "select 16");
        assertQuery("select DATE_PART('ISODOW', DATE '2010-09-27')", "select 1");
        assertQuery("select DATE_PART('DOY', DATE '2001-02-16')", "select 47");
        assertQuery("select DATE_PART('MONTH', DATE '2001-02-16')", "select 2");
        assertQuery("select DATE_PART('QUARTER', DATE '2001-02-16')", "select 1");
        assertQuery("select DATE_PART('ISOWEEK', DATE '2005-01-01')", "select 53");
        assertQuery("select DATE_PART('YEAR', DATE '2001-02-16')", "select 2001");

        assertQuery("select DATE_PART('DAY', TIMESTAMP '2001-02-16 20:38:40')", "select 16");
        assertQuery("select DATE_PART('ISODOW', TIMESTAMP '2010-09-27 20:38:40')", "select 1");
        assertQuery("select DATE_PART('DOY', TIMESTAMP '2001-02-16 20:38:40')", "select 47");
        assertQuery("select DATE_PART('MONTH', TIMESTAMP '2001-02-16 20:38:40')", "select 2");
        assertQuery("select DATE_PART('QUARTER', TIMESTAMP '2001-02-16 20:38:40')", "select 1");
        assertQuery("select DATE_PART('ISOWEEK', TIMESTAMP '2005-01-01 20:38:40')", "select 53");
        assertQuery("select DATE_PART('YEAR', TIMESTAMP '2001-02-16 20:38:40')", "select 2001");
        assertQuery("select DATE_PART('HOUR', TIMESTAMP '2001-02-16 20:38:40')", "select 20");
        assertQuery("select DATE_PART('MINUTE', TIMESTAMP '2001-02-16 20:38:40')", "select 38");
        assertQuery("select DATE_PART('SECOND', TIMESTAMP '2001-02-16 20:38:40')", "select 40");

        assertQuery("select DATE_PART('DAY', TIMESTAMP '2001-02-16 20:38:40 Asia/Singapore')", "select 16");
        assertQuery("select DATE_PART('ISODOW', TIMESTAMP '2010-09-27 20:38:40 America/Los_Angeles')", "select 1");
        assertQuery("select DATE_PART('DOY', TIMESTAMP '2001-02-16 20:38:40 Europe/Berlin')", "select 47");
        assertQuery("select DATE_PART('MONTH', TIMESTAMP '2001-02-16 20:38:40 Asia/Singapore')", "select 2");
        assertQuery("select DATE_PART('QUARTER', TIMESTAMP '2001-02-16 20:38:40 Europe/Berlin')", "select 1");
        assertQuery("select DATE_PART('ISOWEEK', TIMESTAMP '2005-01-01 20:38:40 America/Los_Angeles')", "select 53");
        assertQuery("select DATE_PART('YEAR', TIMESTAMP '2001-02-16 20:38:40 Asia/Singapore')", "select 2001");
        assertQuery("select DATE_PART('HOUR', TIMESTAMP '2001-02-16 20:38:40 Europe/Berlin')", "select 20");
        assertQuery("select DATE_PART('MINUTE', TIMESTAMP '2001-02-16 20:38:40 Asia/Kathmandu')", "select 38");
        assertQuery("select DATE_PART('SECOND', TIMESTAMP '2001-02-16 20:38:40 America/Los_Angeles')", "select 40");

        assertQuery("select DATE_PART('HOUR', TIME '20:38:40')", "select 20");
        assertQuery("select DATE_PART('MINUTE', TIME '20:38:40')", "select 38");
        assertQuery("select DATE_PART('SECOND', TIME '20:38:40')", "select 40");

        assertQuery("select DATE_PART('HOUR', TIME '20:38:40 Europe/Berlin')", "select 20");
        assertQuery("select DATE_PART('MINUTE', TIME '20:38:40 Asia/Kathmandu')", "select 38");
        assertQuery("select DATE_PART('SECOND', TIME '20:38:40 America/Los_Angeles')", "select 40");
    }

    @Test
    public void testDayOfMonth()
            throws Exception
    {
        assertQuery("select DAYOFMONTH(DATE '2008-01-23')", "select 23");
        assertQuery("select DAYOFMONTH(TIMESTAMP '2008-04-29 23:30:05')", "select 29");
        assertQuery("select DAYOFMONTH(TIMESTAMP '2018-01-15 23:30:05 Asia/Singapore')", "select 15");
    }

    @Test
    public void testDayOfWeekISO()
            throws Exception
    {
        assertQuery("select DAYOFWEEK_ISO(DATE '2011-09-22')", "select 4");
        assertQuery("select DAYOFWEEK_ISO(TIMESTAMP '2000-01-01 23:30:05')", "select 6");
        assertQuery("select DAYOFWEEK_ISO(TIMESTAMP '2011-09-22 23:30:05 Asia/Singapore')", "select 4");
    }

    @Test
    public void testDayOfYear()
            throws Exception
    {
        assertQuery("select DAYOFYEAR(DATE '2011-09-22')", "select 265");
        assertQuery("select DAYOFYEAR(TIMESTAMP '2011-09-22 23:30:05')", "select 265");
        assertQuery("select DAYOFYEAR(TIMESTAMP '2011-09-22 23:30:05 Asia/Singapore')", "select 265");
    }

    @Test
    public void testWeekISO()
            throws Exception
    {
        assertQuery("select WEEK_ISO(DATE '2011-12-28')", "select 52");
        assertQuery("select WEEK_ISO(TIMESTAMP '2012-01-01 23:30:05')", "select 52");
        assertQuery("select WEEK_ISO(TIMESTAMP '2012-01-02 23:30:05 Asia/Singapore')", "select 1");
    }

    @Test
    public void testDateDiff()
            throws Exception
    {
        assertQuery("select DATEDIFF('YEAR', DATE '2005-01-01', DATE '2008-12-31')", "select 3");
        assertQuery("select DATEDIFF('QUARTER', DATE '1995-01-01', DATE '1995-02-02')", "select 0");
        assertQuery("select DATEDIFF('MONTH', DATE '2005-01-01', DATE '2005-02-02')", "select 1");
        assertQuery("select DATEDIFF('DAY', DATE '2008-07-01', DATE '2008-08-01')", "select 31");
        assertQuery("select DATEDIFF('WEEK', DATE '2009-02-22', DATE '2009-02-28')", "select 0");

        assertQuery("select DATEDIFF('YEAR', TIMESTAMP '2008-12-31 23:59:59', TIMESTAMP '2009-01-01 00:00:00')", "select 1");
        assertQuery("select DATEDIFF('QUARTER', TIMESTAMP '1993-01-01 20:38:40', TIMESTAMP '1995-02-02 20:38:40')", "select 8");
        assertQuery("select DATEDIFF('MONTH', TIMESTAMP '1995-02-02 20:38:40', TIMESTAMP '1995-01-01 20:38:40')", "select -1");
        assertQuery("select DATEDIFF('DAY', TIMESTAMP '1993-01-01 20:38:40', TIMESTAMP '1995-02-02 20:38:40')", "select 762");
        assertQuery("select DATEDIFF('WEEK', TIMESTAMP '2000-01-01 20:38:40', TIMESTAMP '2000-01-02 20:38:40')", "select 1");
        assertQuery("select DATEDIFF('HOUR', TIMESTAMP '2009-03-02 00:00:00', TIMESTAMP '2009-02-14 00:00:00')", "select -384");
        assertQuery("select DATEDIFF('MINUTE', TIMESTAMP '1993-01-01 03:00:45', TIMESTAMP '1993-01-01 01:30:21')", "select -90");
        assertQuery("select DATEDIFF('SECOND', TIMESTAMP '2001-02-16 20:38:40', TIMESTAMP '2001-02-16 20:40:40')", "select 120");
        assertQuery("select DATEDIFF('MILLISECOND', TIMESTAMP '2001-02-16 20:38:40.345', TIMESTAMP '2001-02-16 20:38:40.678')", "select 333");

        assertQuery("select DATEDIFF('YEAR', TIMESTAMP '2007-12-31 23:59:59 Asia/Singapore', TIMESTAMP '2008-01-01 00:00:00 Asia/Singapore')", "select 1");
        assertQuery("select DATEDIFF('QUARTER', TIMESTAMP '1993-01-01 20:38:40 America/Los_Angeles', TIMESTAMP '1995-02-02 20:38:40 America/Los_Angeles')", "select 8");
        assertQuery("select DATEDIFF('MONTH', TIMESTAMP '1995-02-02 20:38:40 Europe/Berlin', TIMESTAMP '1995-01-01 20:38:40 Europe/Berlin')", "select -1");
        assertQuery("select DATEDIFF('DAY', TIMESTAMP '1993-01-01 20:38:40 Asia/Singapore', TIMESTAMP '1995-02-02 20:38:40 Asia/Singapore')", "select 762");
        assertQuery("select DATEDIFF('WEEK', TIMESTAMP '2000-01-01 20:38:40 America/Los_Angeles', TIMESTAMP '2000-01-02 20:38:40 America/Los_Angeles')", "select 1");
        assertQuery("select DATEDIFF('HOUR', TIMESTAMP '2009-03-02 00:00:00 Europe/Berlin', TIMESTAMP '2009-02-14 00:00:00 Europe/Berlin')", "select -384");
        assertQuery("select DATEDIFF('MINUTE', TIMESTAMP '1993-01-01 03:00:45 Asia/Singapore', TIMESTAMP '1993-01-01 01:30:21 Asia/Singapore')", "select -90");
        assertQuery("select DATEDIFF('SECOND', TIMESTAMP '2001-02-16 20:38:40 America/Los_Angeles', TIMESTAMP '2001-02-16 20:40:40 America/Los_Angeles')", "select 120");
        assertQuery("select DATEDIFF('MILLISECOND', TIMESTAMP '2001-02-16 20:38:40.345 Europe/Berlin', TIMESTAMP '2001-02-16 20:38:40.678 Europe/Berlin')", "select 333");

        assertQuery("select DATEDIFF('HOUR', TIME '00:00:00', TIME '23:00:00')", "select 23");
        assertQuery("select DATEDIFF('MINUTE', TIME '03:00:45', TIME '01:30:21')", "select -90");
        assertQuery("select DATEDIFF('SECOND', TIME '20:38:40', TIME '20:40:40')", "select 120");
        assertQuery("select DATEDIFF('MILLISECOND', TIME '20:38:40.345', TIME '20:38:40.678')", "select 333");

        assertQuery("select DATEDIFF('HOUR', TIME '00:00:00 Europe/Berlin', TIME '23:00:00 Europe/Berlin')", "select 23");
        assertQuery("select DATEDIFF('MINUTE', TIME '03:00:45 Asia/Singapore', TIME '01:30:21 Asia/Singapore')", "select -90");
        assertQuery("select DATEDIFF('SECOND', TIME '20:38:40 America/Los_Angeles', TIME '20:40:40 America/Los_Angeles')", "select 120");
        assertQuery("select DATEDIFF('MILLISECOND', TIME '20:38:40.345 Asia/Kathmandu', TIME '20:38:40.678 Asia/Kathmandu')", "select 333");
    }

    @Test
    public void testTimestampDiff()
            throws Exception
    {
        assertQuery("select TIMESTAMPDIFF('YEAR', TIMESTAMP '2008-12-31 23:59:59', TIMESTAMP '2009-01-01 00:00:00')", "select 1");
        assertQuery("select TIMESTAMPDIFF('QUARTER', TIMESTAMP '1993-01-01 20:38:40', TIMESTAMP '1995-02-02 20:38:40')", "select 8");
        assertQuery("select TIMESTAMPDIFF('MONTH', TIMESTAMP '1995-02-02 20:38:40', TIMESTAMP '1995-01-01 20:38:40')", "select -1");
        assertQuery("select TIMESTAMPDIFF('DAY', TIMESTAMP '1993-01-01 20:38:40', TIMESTAMP '1995-02-02 20:38:40')", "select 762");
        assertQuery("select TIMESTAMPDIFF('WEEK', TIMESTAMP '2000-01-01 20:38:40', TIMESTAMP '2000-01-02 20:38:40')", "select 1");
        assertQuery("select TIMESTAMPDIFF('HOUR', TIMESTAMP '2009-03-02 00:00:00', TIMESTAMP '2009-02-14 00:00:00')", "select -384");
        assertQuery("select TIMESTAMPDIFF('MINUTE', TIMESTAMP '1993-01-01 03:00:45', TIMESTAMP '1993-01-01 01:30:21')", "select -90");
        assertQuery("select TIMESTAMPDIFF('SECOND', TIMESTAMP '2001-02-16 20:38:40', TIMESTAMP '2001-02-16 20:40:40')", "select 120");
        assertQuery("select TIMESTAMPDIFF('MILLISECOND', TIMESTAMP '2001-02-16 20:38:40.345', TIMESTAMP '2001-02-16 20:38:40.678')", "select 333");

        assertQuery("select TIMESTAMPDIFF('YEAR', TIMESTAMP '2008-12-31 23:59:59 Asia/Singapore', TIMESTAMP '2009-01-01 00:00:00 Asia/Singapore')", "select 1");
        assertQuery("select TIMESTAMPDIFF('QUARTER', TIMESTAMP '1993-01-01 20:38:40 America/Los_Angeles', TIMESTAMP '1995-02-02 20:38:40 America/Los_Angeles')", "select 8");
        assertQuery("select TIMESTAMPDIFF('MONTH', TIMESTAMP '1995-02-02 20:38:40 Europe/Berlin', TIMESTAMP '1995-01-01 20:38:40 Europe/Berlin')", "select -1");
        assertQuery("select TIMESTAMPDIFF('DAY', TIMESTAMP '1993-01-01 20:38:40 Asia/Singapore', TIMESTAMP '1995-02-02 20:38:40 Asia/Singapore')", "select 762");
        assertQuery("select TIMESTAMPDIFF('WEEK', TIMESTAMP '2000-01-01 20:38:40 America/Los_Angeles', TIMESTAMP '2000-01-02 20:38:40 America/Los_Angeles')", "select 1");
        assertQuery("select TIMESTAMPDIFF('HOUR', TIMESTAMP '2009-03-02 00:00:00 Europe/Berlin', TIMESTAMP '2009-02-14 00:00:00 Europe/Berlin')", "select -384");
        assertQuery("select TIMESTAMPDIFF('MINUTE', TIMESTAMP '1993-01-01 03:00:45 Asia/Singapore', TIMESTAMP '1993-01-01 01:30:21 Asia/Singapore')", "select -90");
        assertQuery("select TIMESTAMPDIFF('SECOND', TIMESTAMP '2001-02-16 20:38:40 America/Los_Angeles', TIMESTAMP '2001-02-16 20:40:40 America/Los_Angeles')", "select 120");
        assertQuery("select TIMESTAMPDIFF('MILLISECOND', TIMESTAMP '2001-02-16 20:38:40.345 Europe/Berlin', TIMESTAMP '2001-02-16 20:38:40.678 Europe/Berlin')", "select 333");

        assertQuery("select TIMESTAMPDIFF('HOUR', TIME '00:00:00', TIME '23:00:00')", "select 23");
        assertQuery("select TIMESTAMPDIFF('MINUTE', TIME '03:00:45', TIME '01:30:21')", "select -90");
        assertQuery("select TIMESTAMPDIFF('SECOND', TIME '20:38:40', TIME '20:40:40')", "select 120");
        assertQuery("select TIMESTAMPDIFF('MILLISECOND', TIME '20:38:40.345', TIME '20:38:40.678')", "select 333");

        assertQuery("select TIMESTAMPDIFF('HOUR', TIME '00:00:00 Europe/Berlin', TIME '23:00:00 Europe/Berlin')", "select 23");
        assertQuery("select TIMESTAMPDIFF('MINUTE', TIME '03:00:45 Asia/Singapore', TIME '01:30:21 Asia/Singapore')", "select -90");
        assertQuery("select TIMESTAMPDIFF('SECOND', TIME '20:38:40 America/Los_Angeles', TIME '20:40:40 America/Los_Angeles')", "select 120");
        assertQuery("select TIMESTAMPDIFF('MILLISECOND', TIME '20:38:40.345 Asia/Kathmandu', TIME '20:38:40.678 Asia/Kathmandu')", "select 333");
    }

    @Test
    public void testLastDay()
            throws Exception
    {
        assertQuery("select LAST_DAY(DATE '2008-02-28')", "select DATE '2008-02-29'");
        assertQuery("select LAST_DAY(TIMESTAMP '2003-03-15 20:38:40')", "select DATE '2003-03-31'");
        assertQuery("select LAST_DAY(TIMESTAMP '2003-02-05 20:38:40 Asia/Singapore')", "select DATE '2003-02-28'");
    }

    @Test
    public void testMidnightSeconds()
            throws Exception
    {
        assertQuery("select MIDNIGHT_SECONDS(TIMESTAMP '2003-03-15 02:38:40')", "select 9520");
        assertQuery("select MIDNIGHT_SECONDS(TIMESTAMP '2003-02-05 05:03:25 Asia/Singapore')", "select 18205");
        assertQuery("select MIDNIGHT_SECONDS(TIME '12:34:00.987')", "select 45240");
        assertQuery("select MIDNIGHT_SECONDS(TIME '20:38:40 Asia/Singapore')", "select 74320");
    }

    @Test
    public void testNewTime()
            throws Exception
    {
        assertQuery("select NEW_TIME(TIMESTAMP '2012-05-24 13:48:00 America/New_York', 'America/Los_Angeles')", "select TIMESTAMP '2012-05-24 10:48:00 America/Los_Angeles'");
    }

    @Test
    public void testTimestampTrunc()
            throws Exception
    {
        // day, hour, minute, month, second, week, year
        assertQuery("select TIMESTAMP_TRUNC('DAY', TIMESTAMP '2012-05-24 13:48:05')", "select TIMESTAMP '2012-05-24 00:00:00'");
        assertQuery("select TIMESTAMP_TRUNC('HOUR', TIMESTAMP '2012-05-24 13:48:05')", "select TIMESTAMP '2012-05-24 13:00:00'");
        assertQuery("select TIMESTAMP_TRUNC('MINUTE', TIMESTAMP '2012-05-24 13:48:17')", "select TIMESTAMP '2012-05-24 13:48:00'");
        assertQuery("select TIMESTAMP_TRUNC('MONTH', TIMESTAMP '2012-05-24 13:48:17')", "select TIMESTAMP '2012-05-01 00:00:00'");
        assertQuery("select TIMESTAMP_TRUNC('SECOND', TIMESTAMP '2012-05-24 13:48:17.345')", "select TIMESTAMP '2012-05-24 13:48:17.000'");
        assertQuery("select TIMESTAMP_TRUNC('WEEK', TIMESTAMP '2012-05-24 13:48:17')", "select TIMESTAMP '2012-05-20 00:00:00'");
        assertQuery("select TIMESTAMP_TRUNC('YEAR', TIMESTAMP '2012-05-24 13:48:17')", "select TIMESTAMP '2012-01-01 00:00:00'");
    }

    @Test
    public void testTrunc()
            throws Exception
    {
        // day, hour, minute, month, second, week, year
        assertQuery("select TRUNC('DAY', TIMESTAMP '2012-05-24 13:48:05 Asia/Singapore')", "select TIMESTAMP '2012-05-24 00:00:00 Asia/Singapore'");
        assertQuery("select TRUNC('HOUR', TIMESTAMP '2012-05-24 13:48:05 Asia/Singapore')", "select TIMESTAMP '2012-05-24 13:00:00 Asia/Singapore'");
        assertQuery("select TRUNC('MINUTE', TIMESTAMP '2012-05-24 13:48:17 Asia/Singapore')", "select TIMESTAMP '2012-05-24 13:48:00 Asia/Singapore'");
        assertQuery("select TRUNC('MONTH', TIMESTAMP '2012-05-24 13:48:17 Asia/Singapore')", "select TIMESTAMP '2012-05-01 00:00:00 Asia/Singapore'");
        assertQuery("select TRUNC('SECOND', TIMESTAMP '2012-05-24 13:48:17.345 Asia/Singapore')", "select TIMESTAMP '2012-05-24 13:48:17.000 Asia/Singapore'");
        assertQuery("select TRUNC('WEEK', TIMESTAMP '2012-05-24 13:48:17 Asia/Singapore')", "select TIMESTAMP '2012-05-20 00:00:00 Asia/Singapore'");
        assertQuery("select TRUNC('YEAR', TIMESTAMP '2012-05-24 13:48:17 Asia/Singapore')", "select TIMESTAMP '2012-01-01 00:00:00 Asia/Singapore'");
        assertQuery("select TRUNC('YEAR', TIMESTAMP '2012-05-24 13:48:17 Asia/Singapore')", "select TIMESTAMP '2012-01-01 00:00:00 Asia/Singapore'");

        assertQuery("select TRUNC('DAY', DATE '2012-05-24')", "select TIMESTAMP '2012-05-24 00:00:00'");
        assertQuery("select TRUNC('WEEK', DATE '2012-05-24')", "select TIMESTAMP '2012-05-20 00:00:00 '");
        assertQuery("select TRUNC('MONTH', DATE '2012-05-24')", "select TIMESTAMP '2012-05-01 00:00:00'");
        assertQuery("select TRUNC('QUARTER', DATE '2012-05-24')", "select TIMESTAMP '2012-04-01 00:00:00'");
        assertQuery("select TRUNC('YEAR', DATE '2012-05-24')", "select TIMESTAMP '2012-01-01 00:00:00'");
    }

    @Test
    public void testTimestampAdd()
            throws Exception
    {
        assertQuery("select TIMESTAMPADD('YEAR', 2, TIMESTAMP '2008-12-31 23:59:59')", "select TIMESTAMP '2010-12-31 23:59:59'");
        assertQuery("select TIMESTAMPADD('QUARTER', 5, TIMESTAMP '1993-01-01 20:38:40')", "select TIMESTAMP '1994-04-01 20:38:40'");
        assertQuery("select TIMESTAMPADD('MONTH', -3, TIMESTAMP '1995-02-02 20:38:40')", "select TIMESTAMP '1994-11-02 20:38:40'");
        assertQuery("select TIMESTAMPADD('DAY', 47, TIMESTAMP '1993-01-01 20:38:40')", "select TIMESTAMP '1993-02-17 20:38:40'");
        assertQuery("select TIMESTAMPADD('WEEK', 7, TIMESTAMP '2012-02-21 20:38:40')", "select TIMESTAMP '2012-04-10 20:38:40'");
        assertQuery("select TIMESTAMPADD('HOUR', 19, TIMESTAMP '2009-03-02 20:00:00')", "select TIMESTAMP '2009-03-03 15:00:00'");
        assertQuery("select TIMESTAMPADD('MINUTE', 37, TIMESTAMP '1993-01-01 03:00:45')", "select TIMESTAMP '1993-01-01 03:37:45'");
        assertQuery("select TIMESTAMPADD('SECOND', 100, TIMESTAMP '2001-02-16 20:38:40')", "select TIMESTAMP '2001-02-16 20:40:20'");
        assertQuery("select TIMESTAMPADD('MILLISECOND', 493, TIMESTAMP '2001-02-16 20:38:40.345')", "select TIMESTAMP '2001-02-16 20:38:40.838'");

        assertQuery("select TIMESTAMPADD('YEAR', 2, TIMESTAMP '2008-12-31 23:59:59 Asia/Singapore')", "select TIMESTAMP '2010-12-31 23:59:59 Asia/Singapore'");
        assertQuery("select TIMESTAMPADD('QUARTER', 5, TIMESTAMP '1993-01-01 20:38:40 America/Los_Angeles')", "select TIMESTAMP '1994-04-01 20:38:40 America/Los_Angeles'");
        assertQuery("select TIMESTAMPADD('MONTH', -3, TIMESTAMP '1995-02-02 20:38:40 Asia/Kathmandu')", "select TIMESTAMP '1994-11-02 20:38:40 Asia/Kathmandu'");
        assertQuery("select TIMESTAMPADD('DAY', 47, TIMESTAMP '1993-01-01 20:38:40 Europe/Berlin')", "select TIMESTAMP '1993-02-17 20:38:40 Europe/Berlin'");
        assertQuery("select TIMESTAMPADD('WEEK', 7, TIMESTAMP '2000-01-01 20:38:40 Asia/Singapore')", "select TIMESTAMP '2000-02-19 20:38:40 Asia/Singapore'");
        assertQuery("select TIMESTAMPADD('HOUR', 19, TIMESTAMP '2009-03-02 20:00:00 America/Los_Angeles')", "select TIMESTAMP '2009-03-03 15:00:00 America/Los_Angeles'");
        assertQuery("select TIMESTAMPADD('MINUTE', 37, TIMESTAMP '1993-01-01 03:00:45 Asia/Kathmandu')", "select TIMESTAMP '1993-01-01 03:37:45 Asia/Kathmandu'");
        assertQuery("select TIMESTAMPADD('SECOND', 100, TIMESTAMP '2001-02-16 20:38:40 Europe/Berlin')", "select TIMESTAMP '2001-02-16 20:40:20 Europe/Berlin'");
        assertQuery("select TIMESTAMPADD('MILLISECOND', 493, TIMESTAMP '2001-02-16 20:38:40.345 Asia/Singapore')", "select TIMESTAMP '2001-02-16 20:38:40.838 Asia/Singapore'");
    }

    @Test
    public void testYearISO()
            throws Exception
    {
        assertQuery("select YEAR_ISO(DATE '2011-09-22')", "select 2011");
        assertQuery("select YEAR_ISO(TIMESTAMP '2000-01-01 20:38:40')", "select 1999");
        assertQuery("select YEAR_ISO(TIMESTAMP '2000-02-05 20:38:40 Asia/Singapore')", "select 2000");
    }

    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("America/Los_Angeles");
    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
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

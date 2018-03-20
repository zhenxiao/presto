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

import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Locale.ENGLISH;

/**
 * Defines presto UDFs that are compatible to Vertica date/time/timestamp function names.
 */
public final class VerticaDateTimeFunctions
{
    private VerticaDateTimeFunctions()
    {}

    //ADD_MONTHS

    @Description("Add given number of months to a date. Takes a DATE (format yyyy-mm-dd) and a number of months and returns a DATE")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.DATE)
    public static long addMonthsToDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value)
    {
        return DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("month"), value, date);
    }

    @Description("Add given number of months to a timestamp. Takes a Timestamp and a number of months and returns a timestamp")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long addMonthsToTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.BIGINT) long value)
    {
        return DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("month"), value, timestamp);
    }

    @Description("Add given number of months to a timestamp with time zone. Takes a Timestamp and a number of months and returns a timestamp")
    @ScalarFunction("add_months")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long addMonthsToTimestamp(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType(StandardTypes.BIGINT) long value)
    {
        return DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("month"), value, timestamp);
    }

    //AGE_IN_MONTHS

    @Description("Returns an INTEGER value representing the difference in months between start DATE and end DATE")
    @ScalarFunction(value = "age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnDates(ConnectorSession session, @SqlType(StandardTypes.DATE) long startDate, @SqlType(StandardTypes.DATE) long endDate)
    {
        return DateTimeFunctions.diffDate(session, Slices.utf8Slice("month"), startDate, endDate);
    }

    @Description("Returns an INTEGER value representing the difference in months between start DATE and current DATE")
    @ScalarFunction("age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long startDate)
    {
        long currentDate = DateTimeFunctions.currentDate(session);
        return DateTimeFunctions.diffDate(session, Slices.utf8Slice("month"), startDate, currentDate);
    }

    @Description("Returns an INTEGER value representing the difference in months between start Timestamp and end Timestamp")
    @ScalarFunction(value = "age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnTimestamps(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP) long endTimestamp)
    {
        return DateTimeFunctions.diffTimestamp(session, Slices.utf8Slice("month"), startTimestamp, endTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in months between start timestamp and current timestamp")
    @ScalarFunction("age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp)
    {
        long currentTimestamp = DateTimeFunctions.currentTimestamp(session);
        return DateTimeFunctions.diffTimestamp(session, Slices.utf8Slice("month"), startTimestamp, currentTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in months between start Timestamp with time zone and end Timestamp with time zone")
    @ScalarFunction(value = "age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnTimestampsWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long endTimestamp)
    {
        return DateTimeFunctions.diffTimestampWithTimeZone(Slices.utf8Slice("month"), startTimestamp, endTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in months between start timestamp with time zone and current timestamp with time zone")
    @ScalarFunction("age_in_months")
    @SqlType(StandardTypes.BIGINT)
    public static long monthDiffOnTimestampWithTZ(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp)
    {
        long currentTimestamp = DateTimeFunctions.currentTimestamp(session);
        return DateTimeFunctions.diffTimestampWithTimeZone(Slices.utf8Slice("month"), startTimestamp, currentTimestamp);
    }

    //AGE_IN_YEARS

    @Description("Returns an INTEGER value representing the difference in years between start DATE and end DATE")
    @ScalarFunction(value = "age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnDates(ConnectorSession session, @SqlType(StandardTypes.DATE) long startDate, @SqlType(StandardTypes.DATE) long endDate)
    {
        return DateTimeFunctions.diffDate(session, Slices.utf8Slice("year"), startDate, endDate);
    }

    @Description("Returns an INTEGER value representing the difference in years between start DATE and current DATE")
    @ScalarFunction("age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long startDate)
    {
        long currentDate = DateTimeFunctions.currentDate(session);
        return DateTimeFunctions.diffDate(session, Slices.utf8Slice("year"), startDate, currentDate);
    }

    @Description("Returns an INTEGER value representing the difference in years between start Timestamp and end Timestamp")
    @ScalarFunction(value = "age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnTimestamps(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP) long endTimestamp)
    {
        return DateTimeFunctions.diffTimestamp(session, Slices.utf8Slice("year"), startTimestamp, endTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in years between start timestamp and current timestamp")
    @ScalarFunction("age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp)
    {
        long currentTimestamp = DateTimeFunctions.currentTimestamp(session);
        return DateTimeFunctions.diffTimestamp(session, Slices.utf8Slice("year"), startTimestamp, currentTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in years between start Timestamp with time zone and end Timestamp with time zone")
    @ScalarFunction(value = "age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnTimestampsWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long endTimestamp)
    {
        return DateTimeFunctions.diffTimestampWithTimeZone(Slices.utf8Slice("year"), startTimestamp, endTimestamp);
    }

    @Description("Returns an INTEGER value representing the difference in years between start timestamp with time zone and current timestamp with time zone")
    @ScalarFunction("age_in_years")
    @SqlType(StandardTypes.BIGINT)
    public static long yearDiffOnTimestampWithTZ(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp)
    {
        long currentTimestamp = DateTimeFunctions.currentTimestamp(session);
        return DateTimeFunctions.diffTimestampWithTimeZone(Slices.utf8Slice("year"), startTimestamp, currentTimestamp);
    }

    //DATE_PART

    @Description("Returns the field from a DATE. Supported fields are day, isodow, doy, month, quarter, isoweek, year")
    @ScalarFunction(value = "date_part")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long datePartFromDate(@SqlType("varchar(x)") Slice field, @SqlType(StandardTypes.DATE) long date)
    {
        String fieldString = field.toStringUtf8().toLowerCase(ENGLISH);
        switch (fieldString) {
            case "day":
                return DateTimeFunctions.dayFromDate(date);
            case "isodow":
                return DateTimeFunctions.dayOfWeekFromDate(date);
            case "doy":
                return DateTimeFunctions.dayOfYearFromDate(date);
            case "month":
                return DateTimeFunctions.monthFromDate(date);
            case "quarter":
                return DateTimeFunctions.quarterFromDate(date);
            case "isoweek":
                return DateTimeFunctions.weekFromDate(date);
            case "year":
                return DateTimeFunctions.yearFromDate(date);
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + fieldString + "' is not a valid Time field");
    }

    @Description("Returns the field from a Timestamp. Supported fields are day, isodow, doy, hour, minute, month, quarter, second, isoweek, year")
    @ScalarFunction(value = "date_part")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long datePartFromTimestamp(ConnectorSession session, @SqlType("varchar(x)") Slice field, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        String fieldString = field.toStringUtf8().toLowerCase(ENGLISH);
        switch (fieldString) {
            case "day":
                return DateTimeFunctions.dayFromTimestamp(session, timestamp);
            case "isodow":
                return DateTimeFunctions.dayOfWeekFromTimestamp(session, timestamp);
            case "doy":
                return DateTimeFunctions.dayOfYearFromTimestamp(session, timestamp);
            case "hour":
                return DateTimeFunctions.hourFromTimestamp(session, timestamp);
            case "minute":
                return DateTimeFunctions.minuteFromTimestamp(session, timestamp);
            case "month":
                return DateTimeFunctions.monthFromTimestamp(session, timestamp);
            case "quarter":
                return DateTimeFunctions.quarterFromTimestamp(session, timestamp);
            case "second":
                return DateTimeFunctions.secondFromTimestamp(timestamp);
            case "isoweek":
                return DateTimeFunctions.weekFromTimestamp(session, timestamp);
            case "year":
                return DateTimeFunctions.yearFromTimestamp(session, timestamp);
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + fieldString + "' is not a valid Time field");
    }

    @Description("Returns the field from a Timestamp with time zone. Supported fields are day, isodow, doy, hour, minute, month, quarter, second, isoweek, year")
    @ScalarFunction(value = "date_part")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long datePartFromTimestampWithTZ(@SqlType("varchar(x)") Slice field, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        String fieldString = field.toStringUtf8().toLowerCase(ENGLISH);
        switch (fieldString) {
            case "day":
                return DateTimeFunctions.dayFromTimestampWithTimeZone(timestamp);
            case "isodow":
                return DateTimeFunctions.dayOfWeekFromTimestampWithTimeZone(timestamp);
            case "doy":
                return DateTimeFunctions.dayOfYearFromTimestampWithTimeZone(timestamp);
            case "hour":
                return DateTimeFunctions.hourFromTimestampWithTimeZone(timestamp);
            case "minute":
                return DateTimeFunctions.minuteFromTimestampWithTimeZone(timestamp);
            case "month":
                return DateTimeFunctions.monthFromTimestampWithTimeZone(timestamp);
            case "quarter":
                return DateTimeFunctions.quarterFromTimestampWithTimeZone(timestamp);
            case "second":
                return DateTimeFunctions.secondFromTimestampWithTimeZone(timestamp);
            case "isoweek":
                return DateTimeFunctions.weekFromTimestampWithTimeZone(timestamp);
            case "year":
                return DateTimeFunctions.yearFromTimestampWithTimeZone(timestamp);
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + fieldString + "' is not a valid Time field");
    }

    @Description("Returns the field from a Time. Supported fields are hour, minute, second")
    @ScalarFunction(value = "date_part")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long datePartFromTime(ConnectorSession session, @SqlType("varchar(x)") Slice field, @SqlType(StandardTypes.TIME) long time)
    {
        String fieldString = field.toStringUtf8().toLowerCase(ENGLISH);
        switch (fieldString) {
            case "hour":
                return DateTimeFunctions.hourFromTime(session, time);
            case "minute":
                return DateTimeFunctions.minuteFromTime(session, time);
            case "second":
                return DateTimeFunctions.secondFromTime(time);
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + fieldString + "' is not a valid Time field");
    }

    @Description("Returns the field from a Time with Time Zone. Supported fields are hour, minute, second")
    @ScalarFunction(value = "date_part")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long datePartFromTimeWithTZ(ConnectorSession session, @SqlType("varchar(x)") Slice field, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        String fieldString = field.toStringUtf8().toLowerCase(ENGLISH);
        switch (fieldString) {
            case "hour":
                return DateTimeFunctions.hourFromTimeWithTimeZone(time);
            case "minute":
                return DateTimeFunctions.minuteFromTimeWithTimeZone(time);
            case "second":
                return DateTimeFunctions.secondFromTimeWithTimeZone(time);
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + fieldString + "' is not a valid Time field");
    }

    //DAYOFMONTH

    @Description("Returns the day of the month from a DATE")
    @ScalarFunction(value = "DAYOFMONTH")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DateTimeFunctions.dayFromDate(date);
    }

    @Description("Returns the day of the month from a timestamp")
    @ScalarFunction(value = "DAYOFMONTH")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.dayFromTimestamp(session, timestamp);
    }

    @Description("Returns the day of the month from a timestamp with time zone")
    @ScalarFunction(value = "DAYOFMONTH")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.dayFromTimestampWithTimeZone(timestamp);
    }

    //DAYOFWEEK_ISO

    @Description("Returns the ISO day of the week from date. The value ranges from 1 (Monday) to 7 (Sunday).")
    @ScalarFunction(value = "DAYOFWEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DateTimeFunctions.dayOfWeekFromDate(date);
    }

    @Description("Returns the ISO day of the week from timestamp. The value ranges from 1 (Monday) to 7 (Sunday).")
    @ScalarFunction(value = "DAYOFWEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.dayOfWeekFromTimestamp(session, timestamp);
    }

    @Description("Returns the ISO day of the week from timestamp with time zone. The value ranges from 1 (Monday) to 7 (Sunday).")
    @ScalarFunction(value = "DAYOFWEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeekFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.dayOfWeekFromTimestampWithTimeZone(timestamp);
    }

    //DAYOFYEAR

    @Description("Returns the day of the year from date. The value ranges from 1 to 366.")
    @ScalarFunction(value = "DAYOFYEAR")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DateTimeFunctions.dayOfYearFromDate(date);
    }

    @Description("Returns the day of the year from timestamp. The value ranges from 1 to 366.")
    @ScalarFunction(value = "DAYOFYEAR")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.dayOfYearFromTimestamp(session, timestamp);
    }

    @Description("Returns the day of the year from timestamp with time zone. The value ranges from 1 to 366.")
    @ScalarFunction(value = "DAYOFYEAR")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfYearFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.dayOfYearFromTimestampWithTimeZone(timestamp);
    }

    //WEEK_ISO

    @Description("Returns the ISO week of the year from a date. The value ranges from 1 to 53.")
    @ScalarFunction(value = "WEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DateTimeFunctions.weekFromDate(date);
    }

    @Description("Returns the ISO week of the year from a timestamp. The value ranges from 1 to 53.")
    @ScalarFunction(value = "WEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.weekFromTimestamp(session, timestamp);
    }

    @Description("Returns the ISO week of the year from a timestamp with time zone. The value ranges from 1 to 53.")
    @ScalarFunction(value = "WEEK_ISO")
    @SqlType(StandardTypes.BIGINT)
    public static long weekFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.weekFromTimestampWithTimeZone(timestamp);
    }

    //DATEDIFF , TIMESTAMPDIFF for millisecond, second, minute, hour, day, quarter, month, year, week
    /*
        There is difference in the datediff/timestampdiff functions between vertica and Presto. Vertica first truncates the
        datetime objects to the given unit and then does a diff. Whereas Presto directly measures the difference between the
        two datetime objects in the given unit. For example: in Vertica, the difference between 2008/02/21 and 2009/01/22
        in terms of year would be 1. Whereas Presto would count this as zero years since the end date has not reached Feb 21 yet.
        To accomodate this difference, in Presto UDFs, we first truncate the two dates to the given unit and then find the
        difference. In this case 2008/02/21 would be truncated to 2008/01/01 with 'year' as unit. Similarly 2009/01/22 would
        be truncated to 2009/01/01. Now the difference would between the two dates would yield 1 similar to Vertica.

        Another specific corner case is handling the unit 'week'. In Vertica 'week' runs from Sunday to Saturday and in
         Presto 'week' runs from Monday to Sunday. By this definition the above logic would fail when we compute the
         difference between a Satuday and Sunday in terms of week. For example, Vertica would measure the difference in week
         between 2009/02/21 ( Saturday) and 2009/02/22 ( sunday) as 1 week since, a week boundary happens on a Sunday. Whereas
         for the same dates Presto would yield 0 as the difference. To resolve this incompatibility, we truncate non Sunday
         datetime objects

     */

    @Description("Returns an INTEGER value representing the difference in the given unit between start DATE and end DATE")
    @ScalarFunction(value = "datediff")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffOnDates(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long startDate, @SqlType(StandardTypes.DATE) long endDate)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        long truncatedStartDate = DateTimeFunctions.truncateDate(session, unit, startDate);
        long truncatedEndDate = DateTimeFunctions.truncateDate(session, unit, endDate);
        if (unitString.equals("week")) {
            /*
                DO NOT modify if the start/end date is already a Sunday. Else truncation would hav already resulted in a Monday. Subtract one more to make it a Sunday.
             */
            truncatedStartDate = ((DateTimeFunctions.dayOfWeekFromDate(startDate) == 7) ? startDate : DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("day"), -1, truncatedStartDate));
            truncatedEndDate = ((DateTimeFunctions.dayOfWeekFromDate(endDate) == 7) ? endDate : DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("day"), -1, truncatedEndDate));
        }
        return DateTimeFunctions.diffDate(session, unit, truncatedStartDate, truncatedEndDate);
    }

    @Description("Returns an INTEGER value representing the difference in the given unit between start Timestamp and end Timestamp")
    @ScalarFunction(value = "datediff", alias = {"timestampdiff"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffOnTimestamps(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP) long endTimestamp)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        long truncatedStartTs = DateTimeFunctions.truncateTimestamp(session, unit, startTimestamp);
        long truncatedEndTs = DateTimeFunctions.truncateTimestamp(session, unit, endTimestamp);
        if (unitString.equals("week")) {
            /*
                DO NOT modify if the start/end date is already a Sunday. Else truncation would hav already resulted in a Monday. Subtract one more to make it a Sunday.
             */
            truncatedStartTs = ((DateTimeFunctions.dayOfWeekFromTimestamp(session, startTimestamp) == 7) ? startTimestamp : DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("day"), -1, truncatedStartTs));
            truncatedEndTs = ((DateTimeFunctions.dayOfWeekFromTimestamp(session, endTimestamp) == 7) ? endTimestamp : DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("day"), -1, truncatedEndTs));
        }
        return DateTimeFunctions.diffTimestamp(session, unit, truncatedStartTs, truncatedEndTs);
    }

    @Description("Returns an INTEGER value representing the difference in the given unit between start Timestamp with time zone and end Timestamp with time zone")
    @ScalarFunction(value = "datediff", alias = {"timestampdiff"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long daysDiffOnTimestampsWithTZ(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long startTimestamp, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long endTimestamp)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        long truncatedStartTs = DateTimeFunctions.truncateTimestampWithTimezone(unit, startTimestamp);
        long truncatedEndTs = DateTimeFunctions.truncateTimestampWithTimezone(unit, endTimestamp);
        if (unitString.equals("week")) {
            /*
                DO NOT modify if the start/end date is already a Sunday. Else truncation would hav already resulted in a Monday. Subtract one more to make it a Sunday.
             */
            truncatedStartTs = ((DateTimeFunctions.dayOfWeekFromTimestampWithTimeZone(startTimestamp) == 7) ? startTimestamp : DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("day"), -1, truncatedStartTs));
            truncatedEndTs = ((DateTimeFunctions.dayOfWeekFromTimestampWithTimeZone(endTimestamp) == 7) ? endTimestamp : DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("day"), -1, truncatedEndTs));
        }
        return DateTimeFunctions.diffTimestampWithTimeZone(unit, truncatedStartTs, truncatedEndTs);
    }

    @Description("Returns an INTEGER value representing the difference in the given unit between start Time and end Time")
    @ScalarFunction(value = "datediff", alias = {"timestampdiff"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long diffOnTimes(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME) long startTime, @SqlType(StandardTypes.TIME) long endTime)
    {
        long truncatedStartTime = DateTimeFunctions.truncateTime(session, unit, startTime);
        long truncatedEndTime = DateTimeFunctions.truncateTime(session, unit, endTime);
        return DateTimeFunctions.diffTime(session, unit, truncatedStartTime, truncatedEndTime);
    }

    @Description("Returns an INTEGER value representing the difference in the given unit between start Time with time zone and end Time with time zone")
    @ScalarFunction(value = "datediff", alias = {"timestampdiff"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static long daysDiffOnTimesWithTZ(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long startTime, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long endTime)
    {
        long truncatedStartTime = DateTimeFunctions.truncateTimeWithTimeZone(unit, startTime);
        long truncatedEndTime = DateTimeFunctions.truncateTimeWithTimeZone(unit, endTime);
        return DateTimeFunctions.diffTimeWithTimeZone(unit, startTime, endTime);
    }

    //LAST_DAY

    @Description("Returns the last day of the month based on the given date.")
    @ScalarFunction(value = "last_day")
    @SqlType(StandardTypes.DATE)
    public static long lastDayFromDate(ConnectorSession session, @SqlType(StandardTypes.DATE) long inputDate)
    {
        // Add 1 month
        long dateWithOneMonthAdded = DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("month"), 1, inputDate);

        // Truncate to first day of the month
        long truncatedDate = DateTimeFunctions.truncateDate(session, Slices.utf8Slice("month"), dateWithOneMonthAdded);

        // Subtract 1 day
        return DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("day"), -1, truncatedDate);
    }

    @Description("Returns the last day of the month based on the given timestamp.")
    @ScalarFunction(value = "last_day")
    @SqlType(StandardTypes.DATE)
    public static long lastDayFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        // Add 1 month
        long dateWithOneMonthAdded = DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("month"), 1, timestamp);

        // Truncate to first day of the month
        long truncatedTs = DateTimeFunctions.truncateTimestamp(session, Slices.utf8Slice("month"), dateWithOneMonthAdded);

        // Subtract 1 day
        long resultTimestamp = DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("day"), -1, truncatedTs);

        return TimestampOperators.castToDate(session, resultTimestamp);
    }

    @Description("Returns the last day of the month based on the given timestamp with time zone.")
    @ScalarFunction(value = "last_day")
    @SqlType(StandardTypes.DATE)
    public static long lastDayFromTimestampWithTZ(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        // Add 1 month
        long dateWithOneMonthAdded = DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("month"), 1, timestamp);

        // Truncate to first day of the month
        long truncatedTs = DateTimeFunctions.truncateTimestampWithTimezone(Slices.utf8Slice("month"), dateWithOneMonthAdded);

        // Subtract 1 day
        long resultTimestamp = DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("day"), -1, truncatedTs);

        return TimestampWithTimeZoneOperators.castToDate(resultTimestamp);
    }

    //MIDNIGHT_SECONDS

    @Description("Returns a integer that represents the number of seconds between midnight and input timestamp. ")
    @ScalarFunction(value = "midnight_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long midNightSecsFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        // Truncate to midnight
        long truncatedTs = DateTimeFunctions.truncateTimestamp(session, Slices.utf8Slice("day"), timestamp);

        // return the difference
        return DateTimeFunctions.diffTimestamp(session, Slices.utf8Slice("second"), truncatedTs, timestamp);
    }

    @Description("Returns a integer that represents the number of seconds between midnight and input timestamp with timezone. ")
    @ScalarFunction(value = "midnight_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long midNightSecsFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        // Truncate to midnight
        long truncatedTs = DateTimeFunctions.truncateTimestampWithTimezone(Slices.utf8Slice("day"), timestamp);

        // return the difference
        return DateTimeFunctions.diffTimestampWithTimeZone(Slices.utf8Slice("second"), truncatedTs, timestamp);
    }

    @Description("Returns a integer that represents the number of seconds between midnight and input time. ")
    @ScalarFunction(value = "midnight_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long midNightSecsFromTime(ConnectorSession session, @SqlType(StandardTypes.TIME) long time)
    {
        //Get currentTimestamp
        long currentTs = DateTimeFunctions.currentTimestamp(session);

        // Truncate to midnight
        long truncatedTs = DateTimeFunctions.truncateTimestamp(session, Slices.utf8Slice("day"), currentTs);

        // Cast to time
        long truncatedTime = TimestampOperators.castToTime(session, truncatedTs);

        // return the difference
        return DateTimeFunctions.diffTime(session, Slices.utf8Slice("second"), truncatedTime, time);
    }

    @Description("Returns a integer that represents the number of seconds between midnight and input time with timezone. ")
    @ScalarFunction(value = "midnight_seconds")
    @SqlType(StandardTypes.BIGINT)
    public static long midNightSecsFromTimeWithTZ(ConnectorSession session, @SqlType(StandardTypes.TIME_WITH_TIME_ZONE) long time)
    {
        //Truncate given time to hours. Say if input is 07:35:06 this will yield 07:00:00
        long hourRoundedTime = DateTimeFunctions.truncateTimeWithTimeZone(Slices.utf8Slice("hour"), time);

        //Extract Hour from the hourRoundedTime. In this case this will yield 7
        long hours = DateTimeFunctions.hourFromTimeWithTimeZone(hourRoundedTime);

        //Diff hours from the hourRoundedTime to get midnight in the same timezone.
        long midnight = DateTimeFunctions.addFieldValueTimeWithTimeZone(Slices.utf8Slice("hour"), hours * -1, hourRoundedTime);

        // return the difference between midnight and input time.
        return DateTimeFunctions.diffTimeWithTimeZone(Slices.utf8Slice("second"), midnight, time);
    }

    //NEW_TIME

    @Description("Converts a timestamp with timezone value to another time zone. Intervals are not permitted.")
    @ScalarFunction(value = "new_time")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long newTime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        return DateTimeFunctions.timestampAtTimeZone(timestamp, zoneId);
    }

    //TIMEOFDAY

    @Description("Returns a text string representing the time of day.")
    @ScalarFunction(value = "timeofday")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timeOfDay(ConnectorSession session)
    {
        long timestamp = DateTimeFunctions.currentTimestamp(session);
        return DateTimeFunctions.formatDatetimeWithTimeZone(session, timestamp, Slices.utf8Slice("E MMM dd HH:mm:ss.SSSSSS yyyy ZZZ"));
    }

    //TIMESTAMP_TRUNC, TRUNC

    /*
        Truncating to a week in Vertica means truncating to Sunday. Whereas Presto follows ISO standard and truncation
        results in a Monday. To adjust this difference, this Presto UDF will truncate non Sunday dates to the Monday and
         then subtract 1 day from that.
     */
    @Description("Truncates a timestamp to the given unit")
    @ScalarFunction(value = "timestamp_trunc", alias = {"trunc"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateTimestamp(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        if (unitString.equals("week")) {
            if (DateTimeFunctions.dayOfWeekFromTimestamp(session, timestamp) == 7) {
                return timestamp;
            }
            else {
                long truncatedTs = DateTimeFunctions.truncateTimestamp(session, unit, timestamp);
                return DateTimeFunctions.addFieldValueTimestamp(session, Slices.utf8Slice("day"), -1, truncatedTs);
            }
        }
        return DateTimeFunctions.truncateTimestamp(session, unit, timestamp);
    }

    @Description("Truncates a timestamp with time zone to the given unit")
    @ScalarFunction(value = "timestamp_trunc", alias = {"trunc"})
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long truncateTimestampWithTZ(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        if (unitString.equals("week")) {
            if (DateTimeFunctions.dayOfWeekFromTimestampWithTimeZone(timestamp) == 7) {
                return timestamp;
            }
            else {
                long truncatedTs = DateTimeFunctions.truncateTimestampWithTimezone(unit, timestamp);
                return DateTimeFunctions.addFieldValueTimestampWithTimeZone(Slices.utf8Slice("day"), -1, truncatedTs);
            }
        }
        return DateTimeFunctions.truncateTimestampWithTimezone(unit, timestamp);
    }

    //TIMESTAMPADD

    @Description("Adds a specified number of intervals to a timestamp")
    @ScalarFunction("timestampadd")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long addToTimestamp(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value,  @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.addFieldValueTimestamp(session, unit, value, timestamp);
    }

    @Description("Adds a specified number of intervals to a timestamp with time zone")
    @ScalarFunction("timestampadd")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long addToTimestampWithTZ(@SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.BIGINT) long value, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.addFieldValueTimestampWithTimeZone(unit, value, timestamp);
    }

    //TRANSACTION_TIMESTAMP, STATEMENT_TIMESTAMP

    @Description("Returns the current timestamp with time zone")
    @ScalarFunction(value = "transaction_timestamp", alias = {"statement_timestamp"})
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long transactionTimestamp(ConnectorSession session)
    {
        return DateTimeFunctions.currentTimestamp(session);
    }

    //Trunc

    @Description("Truncates a date to the given unit and returns a timestamp")
    @ScalarFunction(value = "trunc")
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long truncateDate(ConnectorSession session, @SqlType("varchar(x)") Slice unit, @SqlType(StandardTypes.DATE) long date)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        if (unitString.equals("week")) {
            if (DateTimeFunctions.dayOfWeekFromDate(date) == 7) {
                return DateOperators.castToTimestamp(session, date);
            }
            else {
                long truncatedDate = DateTimeFunctions.truncateDate(session, unit, date);
                long sundayFromTruncatedDate = DateTimeFunctions.addFieldValueDate(session, Slices.utf8Slice("day"), -1, truncatedDate);
                return DateOperators.castToTimestamp(session, sundayFromTruncatedDate);
            }
        }

        long truncatedDate = DateTimeFunctions.truncateDate(session, unit, date);
        return DateOperators.castToTimestamp(session, truncatedDate);
    }

    //YEAR_ISO

    @Description("Returns the year portion from a date. The return value is based on the ISO 8061 standard. The first week of the ISO year is the week that contains January 4.")
    @ScalarFunction("year_iso")
    @SqlType(StandardTypes.BIGINT)
    public static long isoYearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DateTimeFunctions.yearOfWeekFromDate(date);
    }

    @Description("Returns the year portion from a timestamp. The return value is based on the ISO 8061 standard. The first week of the ISO year is the week that contains January 4.")
    @ScalarFunction("year_iso")
    @SqlType(StandardTypes.BIGINT)
    public static long isoYearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return DateTimeFunctions.yearOfWeekFromTimestamp(session, timestamp);
    }

    @Description("Returns the year portion from a timestamp with time zone. The return value is based on the ISO 8061 standard. The first week of the ISO year is the week that contains January 4.")
    @ScalarFunction("year_iso")
    @SqlType(StandardTypes.BIGINT)
    public static long isoYearFromTimestampWithTZ(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return DateTimeFunctions.yearOfWeekFromTimestampWithTimeZone(timestamp);
    }
}

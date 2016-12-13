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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TimeZoneKey;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.operator.scalar.StringFunctions.stringPosition;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Preconditions.checkArgument;

public final class UserDefinedFunctions
{
    private static final double AVERAGE_RADIUS_OF_EARTH_KM = 6371.009;
    private static final double EPSILON = 1E-6;

    private UserDefinedFunctions() {}

    @SqlNullable
    @Description("Returns the distance(in kilometers) between two points, assuming earth is spherical.")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static Double distance(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat1,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon1,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat2,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon2,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double radiusOfCurvature)
    {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null || radiusOfCurvature == null) {
            return null;
        }

        checkArgument(Math.abs(lat1) < 90.0 + EPSILON && Math.abs(lat2) < 90.0 + EPSILON, "Latitude must be between -90 and 90");
        checkArgument(Math.abs(lon1) < 180.0 + EPSILON && Math.abs(lon2) < 180.0 + EPSILON, "Longitude must be between -180 and 180");

        lat1 = Math.toRadians(lat1);
        lon1 = Math.toRadians(lon1);
        lat2 = Math.toRadians(lat2);
        lon2 = Math.toRadians(lon2);

        double a = Math.pow(Math.sin((lat2 - lat1) / 2.0), 2.0)
                + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin((lon2 - lon1) / 2.0), 2.0);

        // great circle distance in radians
        double angle = 2.0 * Math.asin(Math.min(1.0, Math.sqrt(a)));

        // convert back to degrees
        angle = Math.toDegrees(angle);

        // each degree on a great circle of Earth is 60 nautical miles
        double kmPerDegree = 2.0 * Math.PI * radiusOfCurvature / 360.0;
        return kmPerDegree * angle;
    }

    @SqlNullable
    @Description("Returns the distance(in kilometers) between two points, assuming earth is spherical.")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static Double distance(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat1,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon1,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat2,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon2)
    {
        return distance(lat1, lon1, lat2, lon2, AVERAGE_RADIUS_OF_EARTH_KM);
    }

    @SqlNullable
    @Description("Returns the distance(in kilometers) between two points, using the Vincenty formula.")
    @ScalarFunction("vincenty_distance")
    @SqlType(StandardTypes.DOUBLE)
    public static Double vincentyDistance(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat1,
                                           @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon1,
                                           @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat2,
                                           @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon2)
    {
        if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) {
            return null;
        }

        checkArgument(Math.abs(lat1) < 90.0 + EPSILON && Math.abs(lat2) < 90.0 + EPSILON, "Latitude must be between -90 and 90");
        checkArgument(Math.abs(lon1) < 180.0 + EPSILON && Math.abs(lon2) < 180.0 + EPSILON, "Longitude must be between -180 and 180");

        double a = 6378137;
        double b = 6356752.314245;
        double f = 1 / 298.257223563;
        double l = Math.toRadians(lon2 - lon1);
        double u1 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat1)));
        double u2 = Math.atan((1 - f) * Math.tan(Math.toRadians(lat2)));
        double sinU1 = Math.sin(u1);
        double cosU1 = Math.cos(u1);
        double sinU2 = Math.sin(u2);
        double cosU2 = Math.cos(u2);
        double cosSqAlpha;
        double sinSigma;
        double cos2SigmaM;
        double cosSigma;
        double sigma;

        double lambda = l;
        double lambdaP;
        double iterLimit = 100;

        do {
            double sinLambda = Math.sin(lambda);
            double cosLambda = Math.cos(lambda);
            sinSigma = Math.sqrt((cosU2 * sinLambda)
                    * (cosU2 * sinLambda)
                    + (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda)
                    * (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda));
            if (sinSigma == 0) {
                return 0.0;
            }

            cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda;
            sigma = Math.atan2(sinSigma, cosSigma);
            double sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma;
            cosSqAlpha = 1 - sinAlpha * sinAlpha;
            cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha;

            double c = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha));
            lambdaP = lambda;
            lambda = l + (1 - c) * f * sinAlpha
                    * (sigma + c * sinSigma
                    * (cos2SigmaM + c * cosSigma
                    * (-1 + 2 * cos2SigmaM * cos2SigmaM)));
        } while (Math.abs(lambda - lambdaP) > 1e-12 && --iterLimit > 0);

        if (iterLimit == 0) {
            return 0.0;
        }

        double uSq = cosSqAlpha * (a * a - b * b) / (b * b);
        double aA = 1 + uSq / 16384
                * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)));
        double bB = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)));
        double deltaSigma =
                bB * sinSigma
                        * (cos2SigmaM + bB / 4
                        * (cosSigma
                        * (-1 + 2 * cos2SigmaM * cos2SigmaM) - bB / 6 * cos2SigmaM
                        * (-3 + 4 * sinSigma * sinSigma)
                        * (-3 + 4 * cos2SigmaM * cos2SigmaM)));

        double s = b * aA * (sigma - deltaSigma);

        return s / 1000.0;
    }

    @SqlNullable
    @Description("Determines whether a point (lat,lon) is within a circle of radius" +
            " d kilometers centered at a given point (lat0,lon0).")
    @ScalarFunction("within_circle")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean withinCircle(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat0,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lon0,
                                  @SqlType(StandardTypes.DOUBLE) @SqlNullable Double d)
    {
        if (lat == null || lon == null || lat0 == null || lon0 == null || d == null) {
            return null;
        }
        return distance(lat, lon, lat0, lon0) < d;
    }

    @SqlNullable
    @Description("Returns str, with the first letter of each word in uppercase,"
            + " all other letters in lowercase. Words are delimited by white space.")
    @ScalarFunction("initcap")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice initCap(@SqlType(StandardTypes.VARCHAR) Slice inputStr)
    {
        if (inputStr == null) {
            return null;
        }
        String words = inputStr.toStringUtf8();
        return Slices.utf8Slice(WordUtils.capitalizeFully(words));
    }

    @SqlNullable
    @Description("Returns the substring that matches a regular expression within a string.")
    @ScalarFunction("regexp_substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpSubstr(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long position,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long occurrence,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice regexpModifier,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long capturedSubexp)
    {
        if (source == null || patternStr == null) {
            return null;
        }
        int modifier = getRegexpModifier(regexpModifier);
        String string = source.toStringUtf8().substring(position.intValue() - 1);
        Matcher matcher = Pattern.compile(patternStr.toStringUtf8(), modifier).matcher(string);
        int currentOccurrence = 1;
        while (matcher.find()) {
            if (currentOccurrence == occurrence) {
                try {
                    return Slices.utf8Slice(matcher.group(capturedSubexp.intValue()));
                }
                catch (IndexOutOfBoundsException ex) {
                    return null;
                }
            }
            else {
                currentOccurrence++;
            }
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the substring that matches a regular expression within a string.")
    @ScalarFunction("regexp_substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpSubstr(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long position,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long occurrence,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice regexpModifier)
    {
        return regexpSubstr(source, patternStr, position, occurrence, regexpModifier, Long.valueOf(0));
    }

    @SqlNullable
    @Description("Returns the substring that matches a regular expression within a string.")
    @ScalarFunction("regexp_substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpSubstr(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long position,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long occurrence)
    {
        return regexpSubstr(source, patternStr, position, occurrence, null);
    }

    @SqlNullable
    @Description("Returns the substring that matches a regular expression within a string.")
    @ScalarFunction("regexp_substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpSubstr(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                     @SqlType(StandardTypes.BIGINT) @SqlNullable Long position)
    {
        return regexpSubstr(source, patternStr, position, Long.valueOf(1));
    }

    @SqlNullable
    @Description("Returns the substring that matches a regular expression within a string.")
    @ScalarFunction("regexp_substr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice regexpSubstr(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                     @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr)
    {
        return regexpSubstr(source, patternStr, Long.valueOf(1));
    }

    private static int getRegexpModifier(Slice flags)
    {
        int ret = 0;
        if (flags != null) {
            String modifier = flags.toStringUtf8();
            if (modifier.contains("b")) {
                ret |= Pattern.LITERAL;
            }
            if (modifier.contains("i")) {
                ret |= Pattern.CASE_INSENSITIVE;
            }
            if (modifier.contains("m")) {
                ret |= Pattern.MULTILINE;
            }
            if (modifier.contains("n")) {
                ret |= Pattern.DOTALL;
            }
            if (modifier.contains("x")) {
                ret |= Pattern.COMMENTS;
            }
        }
        return ret;
    }

    @SqlNullable
    @Description("Returns the number times a regular expression matches a string.")
    @ScalarFunction("regexp_count")
    @SqlType(StandardTypes.BIGINT)
    public static Long regexpCount(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                      @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                      @SqlType(StandardTypes.BIGINT) @SqlNullable Long position,
                                      @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice regexpModifier)
    {
        if (source == null || patternStr == null || position == null) {
            return null;
        }
        int count = 0;
        int modifier = getRegexpModifier(regexpModifier);
        String string = source.toStringUtf8().substring(position.intValue() - 1);
        Matcher matcher = Pattern.compile(patternStr.toStringUtf8(), modifier).matcher(string);
        while (matcher.find()) {
            count++;
        }
        return Long.valueOf(count);
    }

    @SqlNullable
    @Description("Returns the number times a regular expression matches a string.")
    @ScalarFunction("regexp_count")
    @SqlType(StandardTypes.BIGINT)
    public static Long regexpCount(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                   @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr,
                                   @SqlType(StandardTypes.BIGINT) @SqlNullable Long position)
    {
        return regexpCount(source, patternStr, position, null);
    }

    @SqlNullable
    @Description("Returns the number times a regular expression matches a string.")
    @ScalarFunction("regexp_count")
    @SqlType(StandardTypes.BIGINT)
    public static Long regexpCount(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice source,
                                      @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice patternStr)
    {
        return regexpCount(source, patternStr, Long.valueOf(1));
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found) in string")
    @ScalarFunction(value = "indexOf", alias = "locate")
    @SqlType(StandardTypes.BIGINT)
    public static long indexOf(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }
        if (string.length() == 0) {
            return 0;
        }
        return stringPosition(string, substring);
    }

    @SqlNullable
    @Description("Returns the integer value of the specified day, where 1 AD is 1.")
    @ScalarFunction("days")
    @SqlType(StandardTypes.BIGINT)
    public static Long days(@SqlType(StandardTypes.TIMESTAMP) @SqlNullable Long timestampLong)
    {
        if (timestampLong == null) {
            return null;
        }
        int[] daysInMonth = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        Timestamp timestamp = new Timestamp(timestampLong);
        int years = timestamp.getYear() + 1900;
        int extraDays = timestamp.getDate();
        for (int i = 0; i < timestamp.getMonth(); i++) {
            extraDays += daysInMonth[i];
        }
        return Long.valueOf((years - 1) * 365 + years / 4 - years / 100 + years / 400 + extraDays);
    }

    @SqlNullable
    @Description("Rounds the specified TIMESTAMP to the precision specified by user.")
    @ScalarFunction("timestamp_round")
    @SqlType(StandardTypes.TIMESTAMP)
    public static Long timestampRound(@SqlType(StandardTypes.TIMESTAMP) @SqlNullable Long timestampLong,
                                      @SqlType(StandardTypes.VARCHAR) Slice precisionSlice)
    {
        if (timestampLong == null) {
            return null;
        }
        Date date = new Date(timestampLong);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        String precision = precisionSlice.toStringUtf8().toLowerCase();

        int field = 0;
        switch (precision) {
            case "syyy":
            case "yyyy":
            case "year":
            case "yyy":
            case "yy":
            case "y":
                field = Calendar.YEAR;
                break;
            case "month":
            case "mon":
            case "mm":
            case "rm":
                field = Calendar.MONTH;
                break;
            case "ddd":
            case "dd":
            case "j":
                field = Calendar.DAY_OF_MONTH;
                break;
            case "hh":
            case "hh12":
            case "hh24":
                field = Calendar.HOUR_OF_DAY;
                break;
            case "mi":
                field = Calendar.MINUTE;
                break;
            case "ss":
                field = Calendar.SECOND;
                break;
            // Starting day of the week
            case "day":
            case "dy":
            case "d":
                Date truncatedDate = DateUtils.round(date, Calendar.DAY_OF_MONTH);
                Calendar truncateDateCalendar = Calendar.getInstance();
                truncateDateCalendar.setTime(truncatedDate);
                truncateDateCalendar.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek());
                return truncateDateCalendar.getTime().getTime();
        }
        if (field > 0) {
            Date truncatedDate = DateUtils.round(date, field);
            return truncatedDate.getTime();
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, precision + " is not a valid precision for function " +
              "TIMESTAMP_ROUND");
        }
    }

    @SqlNullable
    @Description("Rounds the specified TIMESTAMP to the precision specified by user.")
    @ScalarFunction("timestamp_round")
    @SqlType(StandardTypes.TIMESTAMP)
    public static Long timestampRound(@SqlType(StandardTypes.TIMESTAMP) @SqlNullable Long timestampLong)
    {
        return timestampRound(timestampLong, Slices.utf8Slice("day"));
    }

    @SqlNullable
    @Description("Converts a TIMESTAMP value between time zones.")
    @ScalarFunction("to_timestamp_tz")
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static Long toTimestampTimezone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) @SqlNullable Long timestampLong,
                                            @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice timezone1,
                                            @SqlType(StandardTypes.VARCHAR) @SqlNullable Slice timezone2)
    {
        if (timestampLong == null || timezone1 == null || timezone2 == null) {
            return null;
        }
        ZoneId tz1 = getZoneIdFromSlice(timezone1);
        Instant instant = Instant.ofEpochMilli(timestampLong);
        ZonedDateTime passedInZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(session.getTimeZoneKey().getId()));
        passedInZonedDateTime.toEpochSecond();

        ZonedDateTime tz1DateTime = passedInZonedDateTime.withZoneSameLocal(tz1);

        ZoneId tz2 = getZoneIdFromSlice(timezone2);
        ZonedDateTime zonedDateTime = tz1DateTime.withZoneSameInstant(tz2);
        return DateTimeEncoding.packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), TimeZoneKey
          .getTimeZoneKey(tz2.getId()));
    }

    private static ZoneId getZoneIdFromSlice(Slice timezoneSlice)
    {
        String timezoneString = timezoneSlice.toStringUtf8();
        if (ZoneId.SHORT_IDS.containsKey(timezoneString)) {
            timezoneString = ZoneId.SHORT_IDS.get(timezoneString);
        }
        return ZoneId.of(timezoneString);
    }

    @SqlNullable
    @Description("Returns the JSON representation of a lng/lat pair.")
    @ScalarFunction("uber_asgeojson")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice uberAsGeoJSON(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lng,
                                       @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat)
    {
        if (lng == null || lat == null) {
            return null;
        }
        return Slices.utf8Slice("{\"type\":\"Point\",\"coordinates\":[" + lng.toString() + "," + lat.toString() + "]}");
    }

    @SqlNullable
    @Description("Replaces individual characters in string with other characters.")
    @ScalarFunction("translate")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice translate(@SqlType(StandardTypes.VARCHAR) Slice str,
                                      @SqlType(StandardTypes.VARCHAR) Slice from,
                                  @SqlType(StandardTypes.VARCHAR) Slice to)
    {
        return Slices.utf8Slice(StringUtils.replaceChars(str.toStringUtf8(), from.toStringUtf8(), to.toStringUtf8()));
    }
}
